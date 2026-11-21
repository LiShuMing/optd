//! Ordered peer state; rank changes read affected suffixes and seek past unchanged ranges.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::compute::{take, SortOptions};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::WindowFunctionDefinition;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use crate::bag::Bag;
use crate::error::invalid;
use crate::group_state::{supported_key, KeyPolicy};
use crate::planner::plan::RelationOperator;
use crate::state_codec::{
    batch_to_indexed_bags, decode_hex, encode_hex, indexed_bags_to_batch, RankPeerMeta,
    RankSnapshot,
};
use crate::state_directory::StateDirectory;
use crate::{Error, RankWork, Result};

type Key = Vec<u8>;

/// Conservative structural qualification, independent of cardinality and current values.
pub(crate) fn eligible(plan: &LogicalPlan) -> Result<bool> {
    let LogicalPlan::Window(window) = plan else {
        return Ok(false);
    };
    let [expression] = window.window_expr.as_slice() else {
        return Ok(false);
    };
    let mut expression = expression;
    while let Expr::Alias(alias) = expression {
        expression = &alias.expr;
    }
    let Expr::WindowFunction(function) = expression else {
        return Ok(false);
    };
    if function.fun
        != WindowFunctionDefinition::WindowUDF(datafusion::functions_window::rank::rank_udwf())
        || !function.params.partition_by.is_empty()
        || function.params.order_by.is_empty()
        || !function.params.args.is_empty()
        || function.params.null_treatment.is_some()
        || function.params.filter.is_some()
        || function.params.distinct
    {
        return Ok(false);
    }
    for order in &function.params.order_by {
        let ty = order.expr.get_type(window.input.schema())?;
        if !supported_key(
            &ty,
            KeyPolicy {
                binary_strings: true,
            },
        ) && !matches!(ty, DataType::Float32 | DataType::Float64)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Peer payload is immutable across candidates; rank is one plus the preceding row count.
#[derive(Clone, Debug)]
struct Peer {
    rows: Bag,
    count: u64,
    rank: u64,
}

/// Separate physical state for one qualified global rank expression.
#[derive(Clone, Debug)]
pub(crate) struct RankState {
    schema: SchemaRef,
    input_schema: SchemaRef,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    converter: Arc<RowConverter>,
    peers: StateDirectory<Arc<Peer>>,
}

impl RankState {
    /// Build typed order extraction once and count complete input rows per peer.
    pub(crate) fn new(
        ctx: &SessionContext,
        operator: &RelationOperator,
        input: &RecordBatch,
    ) -> Result<Self> {
        let empty = RecordBatch::new_empty(input.schema());
        let state = Self::compile_empty(ctx, operator, input.schema())?;
        Ok(state.update(input, &empty)?.0)
    }

    fn compile_empty(
        ctx: &SessionContext,
        operator: &RelationOperator,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        if !eligible(&operator.template)? {
            return Err(invalid("unqualified rank state"));
        }
        let LogicalPlan::Window(window) = operator.template.as_ref() else {
            return Err(invalid("rank requires window"));
        };
        let mut expression = &window.window_expr[0];
        while let Expr::Alias(alias) = expression {
            expression = &alias.expr;
        }
        let Expr::WindowFunction(function) = expression else {
            return Err(invalid("rank requires function"));
        };
        let session = ctx.state();
        let mut expressions = Vec::new();
        let mut fields = Vec::new();
        for order in &function.params.order_by {
            expressions
                .push(session.create_physical_expr(order.expr.clone(), &operator.inputs[0])?);
            fields.push(SortField::new_with_options(
                order.expr.get_type(window.input.schema())?,
                SortOptions {
                    descending: !order.asc,
                    nulls_first: order.nulls_first,
                },
            ));
        }
        Ok(Self {
            schema: operator.schema(),
            input_schema,
            expressions,
            converter: Arc::new(RowConverter::new(fields)?),
            peers: StateDirectory::new(),
        })
    }

    /// Export ordered rank peers and their weighted input rows.
    pub(crate) fn export_persisted(&self) -> Result<RankSnapshot> {
        let mut peers = Vec::with_capacity(self.peers.len());
        let mut bags = Vec::with_capacity(self.peers.len());
        for (idx, (key, peer)) in self.peers.iter().enumerate() {
            peers.push(RankPeerMeta {
                key_hex: encode_hex(key),
                count: peer.count,
                rank: peer.rank,
            });
            bags.push((idx as u64, &peer.rows));
        }
        Ok(RankSnapshot {
            peers,
            rows_batch: indexed_bags_to_batch(&self.input_schema, &bags)?,
        })
    }

    /// Restore a `RankState` from verified persisted peers without rescanning inputs.
    pub(crate) fn from_persisted(
        ctx: &SessionContext,
        operator: &RelationOperator,
        input_schema: SchemaRef,
        snapshot: RankSnapshot,
    ) -> Result<Self> {
        let mut state = Self::compile_empty(ctx, operator, Arc::clone(&input_schema))?;
        let mut bags = batch_to_indexed_bags(&snapshot.rows_batch, &input_schema)?;
        let mut expected_rank = 1u64;
        for (idx, meta) in snapshot.peers.into_iter().enumerate() {
            if meta.count == 0 || meta.rank != expected_rank {
                return Err(invalid(
                    "persisted rank peer count or rank invariant violated",
                ));
            }
            let key = decode_hex(&meta.key_hex)?;
            let rows = bags
                .remove(&(idx as u64))
                .ok_or_else(|| invalid("missing persisted rank peer rows"))?;
            let volume = rows.materialization_volume()?;
            if volume.rows as u64 != meta.count {
                return Err(invalid("persisted rank peer row count differs"));
            }
            expected_rank = expected_rank
                .checked_add(meta.count)
                .ok_or(Error::Overflow)?;
            if state
                .peers
                .insert(
                    key,
                    Arc::new(Peer {
                        rows,
                        count: meta.count,
                        rank: meta.rank,
                    }),
                )
                .is_some()
            {
                return Err(invalid("duplicate peer key in persisted rank state"));
            }
        }
        Ok(state)
    }

    fn partition(&self, batch: &RecordBatch) -> Result<BTreeMap<Key, Bag>> {
        if batch.schema() != self.input_schema {
            return Err(invalid("rank input schema changed"));
        }
        let arrays = self
            .expressions
            .iter()
            .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
        let encoded = self.converter.convert_columns(&arrays)?;
        let mut groups = BTreeMap::<Key, Vec<u32>>::new();
        for index in 0..batch.num_rows() {
            groups
                .entry(encoded.row(index).as_ref().to_vec())
                .or_default()
                .push(u32::try_from(index).map_err(|_| Error::Overflow)?);
        }
        groups
            .into_iter()
            .map(|(key, indices)| {
                let indices = UInt32Array::from(indices);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|a| take(a.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok((
                    key,
                    Bag::from_batch(&RecordBatch::try_new(batch.schema(), columns)?)?,
                ))
            })
            .collect()
    }

    /// Prefix cardinality is stored in the immediate predecessor, never found by a scan.
    fn rank_before(&self, key: &[u8]) -> Result<u64> {
        if let Some(peer) = self.peers.get(key) {
            return Ok(peer.rank);
        }
        self.peers.predecessor(key).map_or(Ok(1), |(_, peer)| {
            peer.rank.checked_add(peer.count).ok_or(Error::Overflow)
        })
    }

    /// Exact output for a changed peer; multiplicities stay compressed until a consumer asks.
    fn output(&self, peer: &Peer) -> Result<Bag> {
        let entries = peer
            .rows
            .entries()
            .map(|(row, weight)| {
                let mut output = row.to_vec();
                output.push(ScalarValue::UInt64(Some(peer.rank)));
                (output, weight)
            })
            .collect::<Vec<_>>();
        Bag::from_unique_entries(
            self.schema.clone(),
            entries
                .iter()
                .map(|(row, weight)| (row.as_slice(), *weight)),
        )
    }

    /// Prepare an isolated revision and emit only content/rank changes in affected ranges.
    pub(crate) fn update(
        &self,
        positive: &RecordBatch,
        negative: &RecordBatch,
    ) -> Result<(Self, Bag, RankWork)> {
        let mut changes = self.partition(positive)?;
        for (key, rows) in self.partition(negative)? {
            let before = changes
                .remove(&key)
                .unwrap_or_else(|| Bag::empty(self.input_schema.clone()));
            changes.insert(key, before.difference(&rows)?);
        }
        changes.retain(|_, delta| !delta.is_empty());
        let mut next = self.clone();
        let mut pieces = Vec::new();
        let mut work = RankWork {
            delta_input_rows: positive
                .num_rows()
                .checked_add(negative.num_rows())
                .ok_or(Error::Overflow)?,
            changed_peers: changes.len(),
            ..Default::default()
        };
        let Some(first) = changes.keys().next().cloned() else {
            return Ok((next, Bag::empty(self.schema.clone()), work));
        };
        let mut rank = self.rank_before(&first)?;
        let mut old = self
            .peers
            .get(&first)
            .map(|peer| (first.clone(), peer))
            .or_else(|| {
                self.peers
                    .successor(&first)
                    .map(|(key, peer)| (key.clone(), peer))
            });
        let mut changes = changes.into_iter().peekable();
        loop {
            // If prefix cardinality is unchanged, seek straight to the next changed key.
            if old.as_ref().is_some_and(|(_, peer)| peer.rank == rank) {
                let Some((key, _)) = changes.peek() else {
                    break;
                };
                if old.as_ref().is_some_and(|(old_key, _)| old_key < key) {
                    rank = self.rank_before(key)?;
                    old = self
                        .peers
                        .get(key)
                        .map(|peer| (key.clone(), peer))
                        .or_else(|| {
                            self.peers
                                .successor(key)
                                .map(|(key, peer)| (key.clone(), peer))
                        });
                }
            }
            let key = match (old.as_ref(), changes.peek()) {
                (None, None) => break,
                (Some((key, _)), None) | (None, Some((key, _))) => key.clone(),
                (Some((old_key, _)), Some((key, _))) => old_key.min(key).clone(),
            };
            work.visited_peers = work.visited_peers.checked_add(1).ok_or(Error::Overflow)?;
            let previous = old
                .as_ref()
                .filter(|(old_key, _)| *old_key == key)
                .map(|(_, peer)| *peer);
            let rows = if changes.peek().is_some_and(|(k, _)| *k == key) {
                // peek established that the next change exists.
                let (_, change) = changes
                    .next()
                    .ok_or_else(|| invalid("missing rank change"))?;
                previous.map_or_else(|| Ok(change.clone()), |peer| peer.rows.add(&change))?
            } else {
                previous
                    .ok_or_else(|| invalid("rank peer missing"))?
                    .rows
                    .clone()
            };
            rows.validate_relation()?;
            let count = rows.entries().try_fold(0u64, |sum, (_, weight)| {
                sum.checked_add(u64::try_from(weight).map_err(|_| Error::Overflow)?)
                    .ok_or(Error::Overflow)
            })?;
            let peer = Peer { rows, count, rank };
            if let Some(previous) = previous {
                work.historical_input_rows = work
                    .historical_input_rows
                    .checked_add(usize::try_from(previous.count).map_err(|_| Error::Overflow)?)
                    .ok_or(Error::Overflow)?;
                pieces.push(Bag::empty(self.schema.clone()).difference(&self.output(previous)?)?);
            }
            if count == 0 {
                next.peers.remove(&key);
            } else {
                pieces.push(self.output(&peer)?);
                next.peers.insert(key.clone(), Arc::new(peer));
            }
            rank = rank.checked_add(count).ok_or(Error::Overflow)?;
            old = self
                .peers
                .successor(&key)
                .map(|(key, peer)| (key.clone(), peer));
        }
        // Balanced signed merging avoids quadratic copying on necessary large suffixes.
        while pieces.len() > 1 {
            pieces = pieces
                .chunks(2)
                .map(|pair| {
                    if pair.len() == 2 {
                        pair[0].add(&pair[1])
                    } else {
                        Ok(pair[0].clone())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
        }
        let delta = pieces
            .pop()
            .unwrap_or_else(|| Bag::empty(self.schema.clone()));
        work.directory_entries_copied = next.peers.copied_entries()?;
        work.output_delta_rows = delta.len();
        Ok((next, delta, work))
    }
}
