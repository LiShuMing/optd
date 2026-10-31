//! Derived per-row residual counts and bounded SQL predicate evaluation.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{NullEquality, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use super::super::{Bucket, Key, KeyedDelta, KeyedState};
use super::{invalid, Output};
use crate::bag::Bag;
use crate::planner::plan::RelationOperator;
use crate::state_directory::StateDirectory;
use crate::{Error, Result};

type Counts = HashMap<Vec<ScalarValue>, i64>;
const PREDICATE_BATCH_ROWS: usize = 1024;

/// Compiled SQL residual plus candidate-shared per-key full-row match counts.
#[derive(Clone, Debug)]
pub(in crate::keyed_state) struct ResidualState {
    // Immutable SQL predicate, compiled against the combined input schema.
    expression: Arc<dyn PhysicalExpr>,
    pair_schema: SchemaRef,
    // Full input-row identity -> nonnegative peer multiplicity, including zero.
    counts: [StateDirectory<Arc<Counts>>; 2],
}

/// Accumulate signed peer-count deltas; cancellations are applied before crossings.
fn count_add(counts: &mut Counts, row: &[ScalarValue], change: i64) -> Result<()> {
    let count = counts
        .get(row)
        .copied()
        .unwrap_or_default()
        .checked_add(change)
        .ok_or(Error::Overflow)?;
    counts.insert(row.to_vec(), count);
    Ok(())
}

/// Keep zero-count identities so first matches can retract anti/outer membership.
fn zero_counts(bucket: Option<&Bucket>) -> Counts {
    bucket.map_or_else(Counts::new, |b| {
        b.bag.entries().map(|(row, _)| (row.to_vec(), 0)).collect()
    })
}

impl ResidualState {
    /// Full bootstrap is deliberate; refresh never repeats unchanged row pairs.
    pub(in crate::keyed_state) fn initialize(
        state: &KeyedState,
        ctx: &SessionContext,
        operator: &RelationOperator,
    ) -> Result<Option<Self>> {
        let LogicalPlan::Join(join) = operator.template.as_ref() else {
            return Err(invalid("residual state without join"));
        };
        let Some(filter) = &join.filter else {
            return Ok(None);
        };
        let schema = operator.inputs[0].join(&operator.inputs[1])?;
        let mut result = Self {
            expression: ctx.state().create_physical_expr(filter.clone(), &schema)?,
            pair_schema: Arc::new(schema.as_arrow().clone()),
            counts: [StateDirectory::new(), StateDirectory::new()],
        };
        let keys = state
            .inputs
            .iter()
            .flat_map(StateDirectory::keys)
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in keys {
            let left = state.inputs[0].get(&key).map(AsRef::as_ref);
            let right = state.inputs[1].get(&key).map(AsRef::as_ref);
            let mut counts = [zero_counts(left), zero_counts(right)];
            if let (Some(left), Some(right)) = (left, right) {
                if join.null_equality == NullEquality::NullEqualsNull
                    || (!left.key_has_null && !right.key_has_null)
                {
                    for (row, weight) in left.bag.entries() {
                        result.probe(0, row, &right.bag, |peer, peer_weight, matched| {
                            if matched {
                                count_add(&mut counts[0], row, peer_weight)?;
                                count_add(&mut counts[1], peer, weight)?;
                            }
                            Ok(())
                        })?;
                    }
                }
            }
            for (side, map) in counts.into_iter().enumerate() {
                if !map.is_empty() {
                    result.counts[side].insert(key.clone(), Arc::new(map));
                }
            }
        }
        Ok(Some(result))
    }

    /// Evaluate at most one bounded chunk at a time, preserving SQL NULL semantics.
    fn probe(
        &self,
        side: usize,
        row: &[ScalarValue],
        peers: &Bag,
        mut visit: impl FnMut(&[ScalarValue], i64, bool) -> Result<()>,
    ) -> Result<()> {
        let mut peers = peers.entries();
        loop {
            let chunk = peers
                .by_ref()
                .take(PREDICATE_BATCH_ROWS)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                return Ok(());
            }
            let mut columns = Vec::with_capacity(self.pair_schema.fields().len());
            for column in 0..self.pair_schema.fields().len() {
                let values = chunk.iter().map(|(peer, _)| {
                    let (left, right) = if side == 0 {
                        (row, *peer)
                    } else {
                        (*peer, row)
                    };
                    if column < left.len() {
                        left[column].clone()
                    } else {
                        right[column - left.len()].clone()
                    }
                });
                columns.push(ScalarValue::iter_to_array(values)?);
            }
            let batch = RecordBatch::try_new(self.pair_schema.clone(), columns)?;
            let array = self.expression.evaluate(&batch)?.into_array(chunk.len())?;
            let booleans = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("residual predicate did not return Boolean"))?;
            for (i, (peer, weight)) in chunk.into_iter().enumerate() {
                visit(peer, weight, !booleans.is_null(i) && booleans.value(i))?;
            }
        }
    }

    /// Two complete one-sided stages update private counts before installation.
    pub(super) fn maintain(
        &mut self,
        before: &KeyedState,
        after: &KeyedState,
        changes: &[BTreeMap<Key, KeyedDelta>],
        equality: NullEquality,
        output: &mut Output,
    ) -> Result<()> {
        // After the left stage, both count maps describe A-after/B-before.
        // The right stage then advances both maps to the final input endpoints.
        for (side, deltas) in changes.iter().enumerate() {
            for (key, delta) in deltas {
                let peer_state = if side == 0 { before } else { after };
                let peer = peer_state.inputs[1 - side]
                    .get(key)
                    .map(AsRef::as_ref)
                    .filter(|p| {
                        equality == NullEquality::NullEqualsNull
                            || (!p.key_has_null && !delta.key_has_null)
                    });
                output.work.count_probes = output
                    .work
                    .count_probes
                    .checked_add(1)
                    .ok_or(Error::Overflow)?;
                // Private full count-map copies are a real touched-bucket cost;
                // untouched directory branches and their map payloads stay shared.
                let mut maps = [Counts::new(), Counts::new()];
                for (port, map) in maps.iter_mut().enumerate() {
                    if let Some(old) = self.counts[port].get(key) {
                        output.work.count_map_entries_copied = output
                            .work
                            .count_map_entries_copied
                            .checked_add(old.len())
                            .ok_or(Error::Overflow)?;
                        *map = old.as_ref().clone();
                    }
                }
                let mut peer_changes = Counts::new();
                for (row, weight) in delta.bag.entries() {
                    output.work.changed_entries = output
                        .work
                        .changed_entries
                        .checked_add(1)
                        .ok_or(Error::Overflow)?;
                    let mut own_count = 0_i64;
                    if let Some(peer) = peer {
                        self.probe(side, row, &peer.bag, |other, count, matched| {
                            output.work.peer_entries_visited = output
                                .work
                                .peer_entries_visited
                                .checked_add(1)
                                .ok_or(Error::Overflow)?;
                            output.work.residual_candidate_pairs = output
                                .work
                                .residual_candidate_pairs
                                .checked_add(1)
                                .ok_or(Error::Overflow)?;
                            output.work.residual_predicate_rows = output
                                .work
                                .residual_predicate_rows
                                .checked_add(1)
                                .ok_or(Error::Overflow)?;
                            if matched {
                                own_count = own_count.checked_add(count).ok_or(Error::Overflow)?;
                                count_add(&mut peer_changes, other, weight)?;
                                if output.outer() {
                                    output.work.matched_pairs = output
                                        .work
                                        .matched_pairs
                                        .checked_add(1)
                                        .ok_or(Error::Overflow)?;
                                    let pair = if side == 0 {
                                        [row, other].concat()
                                    } else {
                                        [other, row].concat()
                                    };
                                    output.emit(
                                        pair,
                                        weight.checked_mul(count).ok_or(Error::Overflow)?,
                                    )?;
                                }
                            }
                            Ok(())
                        })?;
                    }
                    // Existing changed rows must agree with their retained count
                    // at this exact stage, including zero-count/NULL-only peers.
                    if maps[side].get(row).is_some_and(|old| *old != own_count) {
                        return Err(invalid("cached own match count differs from fixed peers"));
                    }
                    if output.membership(side).visible(own_count > 0) {
                        output.own(side, row, weight)?;
                    }
                    if after.inputs[side]
                        .get(key)
                        .is_some_and(|b| b.bag.weight(row) > 0)
                    {
                        maps[side].insert(row.to_vec(), own_count);
                    } else {
                        maps[side].remove(row);
                    }
                    output.work.match_count_updates = output
                        .work
                        .match_count_updates
                        .checked_add(1)
                        .ok_or(Error::Overflow)?;
                }
                // Decide crossings only after every signed changed row in this
                // stage contributed; transient per-row order has no SQL meaning.
                for (row, change) in peer_changes {
                    if change == 0 {
                        continue;
                    }
                    let old = maps[1 - side]
                        .get_mut(&row)
                        .ok_or_else(|| invalid("peer row has no retained match count"))?;
                    let new = old.checked_add(change).ok_or(Error::Overflow)?;
                    if new < 0 {
                        return Err(invalid("negative residual match count"));
                    }
                    if (*old > 0) != (new > 0) {
                        output.work.zero_crossings = output
                            .work
                            .zero_crossings
                            .checked_add(1)
                            .ok_or(Error::Overflow)?;
                        let membership = output.membership(1 - side);
                        let sign = i64::from(membership.visible(new > 0))
                            - i64::from(membership.visible(*old > 0));
                        if sign != 0 {
                            let weight = peer
                                .ok_or_else(|| invalid("missing crossing peer bucket"))?
                                .bag
                                .weight(&row);
                            output.own(
                                1 - side,
                                &row,
                                sign.checked_mul(weight).ok_or(Error::Overflow)?,
                            )?;
                        }
                    }
                    *old = new;
                    output.work.match_count_updates = output
                        .work
                        .match_count_updates
                        .checked_add(1)
                        .ok_or(Error::Overflow)?;
                }
                for (port, map) in maps.into_iter().enumerate() {
                    if map.is_empty() {
                        self.counts[port].remove(key);
                    } else {
                        self.counts[port].insert(key.clone(), Arc::new(map));
                    }
                }
            }
        }
        output.work.count_directory_entries_copied =
            self.counts.iter().try_fold(0_usize, |total, d| {
                total
                    .checked_add(d.copied_entries()?)
                    .ok_or(Error::Overflow)
            })?;
        Ok(())
    }
}
