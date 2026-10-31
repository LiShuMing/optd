//! Match-count equijoin transitions over the declared Before/After keyed state.
//!
//! For pure equijoins, each eligible row shares its bucket's peer match count.
//! Residual predicates use per-row counts in the dedicated child module.
//! Semi/anti joins therefore read peers only at zero crossings. Outer joins
//! additionally visit changed-row/peer pairs to emit their necessary output.

use std::collections::HashMap;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{NullEquality, ScalarValue};
use datafusion::logical_expr::{JoinType, LogicalPlan};

use super::{invalid, Bucket, KeyedUpdate};
use crate::bag::Bag;
use crate::metrics::MatchJoinWork;
use crate::planner::plan::RelationOperator;
use crate::{Error, Result};

mod residual;
pub(super) use residual::ResidualState;

/// Whether one side emits its own row at a given peer-count positivity.
#[derive(Clone, Copy)]
enum Membership {
    None,
    Matched,
    Unmatched,
}
impl Membership {
    fn visible(self, matched: bool) -> bool {
        match self {
            Self::None => false,
            Self::Matched => matched,
            Self::Unmatched => !matched,
        }
    }
}

/// Original left/right output orientation, including typed outer NULL columns.
struct Output {
    kind: JoinType,
    nulls: [Vec<ScalarValue>; 2],
    rows: HashMap<Vec<ScalarValue>, i64>,
    work: MatchJoinWork,
}
impl Output {
    /// Consolidate typed signed output after both stages have completed.
    fn finish(self, schema: SchemaRef) -> Result<(Bag, MatchJoinWork)> {
        let bag = Bag::from_unique_entries(
            schema,
            self.rows
                .iter()
                .map(|(row, weight)| (row.as_slice(), *weight)),
        )?;
        Ok((bag, self.work))
    }
    fn membership(&self, side: usize) -> Membership {
        match (self.kind, side) {
            (JoinType::LeftSemi, 0) | (JoinType::RightSemi, 1) => Membership::Matched,
            (JoinType::Left | JoinType::LeftAnti | JoinType::Full, 0)
            | (JoinType::Right | JoinType::RightAnti | JoinType::Full, 1) => Membership::Unmatched,
            _ => Membership::None,
        }
    }
    fn outer(&self) -> bool {
        matches!(self.kind, JoinType::Left | JoinType::Right | JoinType::Full)
    }
    fn emit(&mut self, row: Vec<ScalarValue>, weight: i64) -> Result<()> {
        if weight == 0 {
            return Ok(());
        }
        self.work.emitted_contributions = self
            .work
            .emitted_contributions
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        let next = self
            .rows
            .get(&row)
            .copied()
            .unwrap_or_default()
            .checked_add(weight)
            .ok_or(Error::Overflow)?;
        if next == 0 {
            self.rows.remove(&row);
        } else {
            self.rows
                .try_reserve(1)
                .map_err(|_| invalid("match-count output allocation failed"))?;
            self.rows.insert(row, next);
        }
        Ok(())
    }
    fn own(&mut self, side: usize, row: &[ScalarValue], weight: i64) -> Result<()> {
        let output = if !self.outer() {
            row.to_vec()
        } else if side == 0 {
            [row, &self.nulls[1]].concat()
        } else {
            [&self.nulls[0], row].concat()
        };
        self.emit(output, weight)
    }

    /// Apply one side's net changes. `peer` stays fixed throughout this stage.
    fn stage(
        &mut self,
        side: usize,
        delta: &Bag,
        peer: Option<&Bucket>,
        old_count: usize,
        new_count: usize,
    ) -> Result<()> {
        self.work.count_probes = self
            .work
            .count_probes
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        let peer_matched = peer.is_some_and(|b| b.batch.num_rows() > 0);
        let own_membership = self.membership(side);
        for (row, weight) in delta.entries() {
            self.work.changed_entries = self
                .work
                .changed_entries
                .checked_add(1)
                .ok_or(Error::Overflow)?;
            if own_membership.visible(peer_matched) {
                self.own(side, row, weight)?;
            }
            if self.outer() {
                if let Some(peer) = peer {
                    for (other, count) in peer.bag.entries() {
                        self.work.peer_entries_visited = self
                            .work
                            .peer_entries_visited
                            .checked_add(1)
                            .ok_or(Error::Overflow)?;
                        self.work.matched_pairs = self
                            .work
                            .matched_pairs
                            .checked_add(1)
                            .ok_or(Error::Overflow)?;
                        let pair = if side == 0 {
                            [row, other].concat()
                        } else {
                            [other, row].concat()
                        };
                        self.emit(pair, weight.checked_mul(count).ok_or(Error::Overflow)?)?;
                    }
                }
            }
        }
        let before = old_count > 0;
        let after = new_count > 0;
        if before != after {
            self.work.zero_crossings = self
                .work
                .zero_crossings
                .checked_add(1)
                .ok_or(Error::Overflow)?;
            let peer_membership = self.membership(1 - side);
            let weight = i64::from(peer_membership.visible(after))
                - i64::from(peer_membership.visible(before));
            if weight != 0 {
                if let Some(peer) = peer {
                    for (row, count) in peer.bag.entries() {
                        self.work.peer_entries_visited = self
                            .work
                            .peer_entries_visited
                            .checked_add(1)
                            .ok_or(Error::Overflow)?;
                        self.own(
                            1 - side,
                            row,
                            count.checked_mul(weight).ok_or(Error::Overflow)?,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl KeyedUpdate<'_> {
    /// Execute the two ordered one-sided transitions, never snapshot diffing.
    pub(crate) fn maintain_match_join(
        &mut self,
        operator: &RelationOperator,
    ) -> Result<(Bag, MatchJoinWork)> {
        let LogicalPlan::Join(join) = operator.template.as_ref() else {
            return Err(invalid("match-count operator is not a join"));
        };
        let update = self
            .match_update
            .as_ref()
            .ok_or_else(|| invalid("match-count changes were not retained"))?;
        let before = self
            .before
            .ok_or_else(|| invalid("match-count Before is not bound"))?;
        if !self.after_readable || update.changes.len() != 2 || join.null_aware {
            return Err(invalid("unqualified match-count state/operator"));
        }
        let mut nulls = [Vec::new(), Vec::new()];
        for (side, schema) in operator.inputs.iter().enumerate() {
            nulls[side] = schema
                .fields()
                .iter()
                .map(|f| ScalarValue::try_from(f.data_type()))
                .collect::<std::result::Result<_, _>>()?;
        }
        let mut output = Output {
            kind: join.join_type,
            nulls,
            rows: HashMap::new(),
            work: update.work.clone(),
        };
        if let Some(residual) = &self.state.residual {
            let mut next = residual.clone();
            next.maintain(
                before,
                &self.state,
                &update.changes,
                join.null_equality,
                &mut output,
            )?;
            self.state.residual = Some(next);
            return output.finish(operator.schema());
        }
        if join.filter.is_some() {
            return Err(invalid("missing residual match-count state"));
        }
        // Delta-left with Before-right, then After-left with Delta-right. This
        // ordering counts the cross term once and avoids transient row ordering.
        for (side, deltas) in update.changes.iter().enumerate() {
            for (key, delta) in deltas {
                let matched_key =
                    !delta.key_has_null || join.null_equality == NullEquality::NullEqualsNull;
                let peer_state = if side == 0 { before } else { &self.state };
                let peer = peer_state.inputs[1 - side]
                    .get(key)
                    .map(AsRef::as_ref)
                    .filter(|bucket| {
                        matched_key
                            && (!bucket.key_has_null
                                || join.null_equality == NullEquality::NullEqualsNull)
                    });
                let count = |state: &super::KeyedState| {
                    if matched_key {
                        state.inputs[side]
                            .get(key)
                            .map_or(0, |b| b.batch.num_rows())
                    } else {
                        0
                    }
                };
                output.stage(side, &delta.bag, peer, count(before), count(&self.state))?;
            }
        }
        output.finish(operator.schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn pair_products_and_colliding_output_weights_refuse_overflow() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let row = vec![ScalarValue::Int64(Some(1))];
        let peer = Bucket {
            bag: Bag::from_unique_entries(schema.clone(), [(row.as_slice(), i64::MAX)])?,
            // No expanded allocation is needed to test the checked product kernel.
            batch: RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])?,
            key_has_null: false,
        };
        let delta = Bag::from_unique_entries(schema, [(row.as_slice(), 2)])?;
        let mut output = Output {
            kind: JoinType::Full,
            nulls: [vec![], vec![]],
            rows: HashMap::new(),
            work: MatchJoinWork::default(),
        };
        assert!(matches!(
            output.stage(0, &delta, Some(&peer), 1, 3),
            Err(Error::Overflow)
        ));
        output.emit(row.clone(), i64::MAX)?;
        assert!(matches!(output.emit(row, 1), Err(Error::Overflow)));
        Ok(())
    }
}
