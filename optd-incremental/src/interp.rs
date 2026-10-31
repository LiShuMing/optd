// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! A reference interpreter for incremental plans.
//!
//! This exists so the framework can be **checked**, not just compiled: the
//! differential test maintains a view incrementally and compares it against an
//! evaluation of the same plan over the current data. Without that comparison the
//! rules are "plausible", not verified.
//!
//! It is deliberately naive (nested-loop join, no indexes). Milestone 1 is about
//! whether the *algebra* is right; performance is a later concern, and an
//! optimized evaluator would make it harder to tell an algebra bug from an
//! execution bug.

use std::collections::BTreeMap;

use crate::change::{Multiset, Position, Row, TableId, Value};
use crate::plan::{children_of, AggKind, Expr, IncNodeType, IncPlanNode};
use crate::source::DeltaSource;

/// Per-table refresh ranges: `(from, to]` for each base table.
pub type Ranges = BTreeMap<TableId, (Position, Position)>;

/// Immutable source endpoints and committed view state for reference evaluation.
/// Ordinary scans use `position`; delta scans use their per-table `ranges`.
pub struct EvalContext<'a> {
    /// Source observation that retains the requested snapshots and change ranges.
    pub source: &'a dyn DeltaSource,
    /// Snapshot endpoint pinned for all ordinary scans.
    pub position: Position,
    /// Per-table change ranges; an absent table contributes no delta rows.
    pub ranges: &'a Ranges,
    /// The view's current contents, for `StoredView`.
    pub stored_view: &'a Multiset,
}

/// Evaluate a concrete reference plan without mutating source or committed state.
///
/// Rejects invalid arity, unresolved non-scan delta markers, invalid column
/// references and checked arithmetic failures. This fully materializing evaluator
/// is a semantic oracle, not the typed SQL executor or an efficiency guarantee.
pub fn eval(plan: &IncPlanNode, ctx: &EvalContext) -> Result<Multiset, String> {
    let kids = children_of(plan);
    let arity = match &plan.typ {
        IncNodeType::Scan(_) | IncNodeType::EmptyRelation | IncNodeType::StoredView => 0,
        IncNodeType::Join(_) | IncNodeType::Union | IncNodeType::MergeAgg { .. } => 2,
        _ => 1,
    };
    if kids.len() != arity || kids.len() != plan.children.len() {
        return Err("invalid concrete operator arity".into());
    }
    match &plan.typ {
        IncNodeType::Scan(t) => ctx
            .source
            .snapshot_at(*t, ctx.position)
            .map_err(|e| e.to_string()),

        // The resolved marker: read the change feed for this table in range.
        IncNodeType::Delta => match kids.first().map(|k| &k.typ) {
            Some(IncNodeType::Scan(t)) => match ctx.ranges.get(t) {
                Some((from, to)) => ctx
                    .source
                    .changes(*t, *from, *to)
                    .map_err(|e| e.to_string()),
                // A table with no recorded range contributes no changes.
                None => Ok(Multiset::new()),
            },
            other => Err(format!(
                "unresolved delta marker on `{}`",
                other.map(|t| t.to_string()).unwrap_or_default()
            )),
        },

        IncNodeType::EmptyRelation => Ok(Multiset::new()),
        IncNodeType::StoredView => Ok(ctx.stored_view.clone()),

        IncNodeType::Projection(cols) => {
            let input = eval(kids[0], ctx)?;
            let mut out = Multiset::new();
            for (row, w) in input.iter() {
                let projected: Row = cols
                    .iter()
                    .map(|c| column(row, *c).cloned())
                    .collect::<Result<_, _>>()?;
                out.try_add_row(projected, *w).map_err(arithmetic)?;
            }
            Ok(out)
        }

        IncNodeType::Filter => {
            let pred = plan
                .predicates
                .first()
                .map(|p| p.typ.clone())
                .ok_or("filter without predicate")?;
            let input = eval(kids[0], ctx)?;
            let mut out = Multiset::new();
            for (row, w) in input.iter() {
                // SQL WHERE keeps only rows where the predicate is TRUE, so
                // UNKNOWN (NULL) is dropped — the same rule a rebuild would apply.
                if matches!(eval_expr(&pred, row)?, Value::Int(1)) {
                    out.try_add_row(row.clone(), *w).map_err(arithmetic)?;
                }
            }
            Ok(out)
        }

        IncNodeType::Join(keys) => {
            let left = eval(kids[0], ctx)?;
            let right = eval(kids[1], ctx)?;
            let mut out = Multiset::new();
            for (lrow, lw) in left.iter() {
                for (rrow, rw) in right.iter() {
                    let mut matched = true;
                    for (lc, rc) in keys {
                        let a = column(lrow, *lc)?;
                        let b = column(rrow, *rc)?;
                        matched &= a == b && *a != Value::Null;
                    }
                    if matched {
                        let mut row = lrow.clone();
                        row.extend(rrow.iter().cloned());
                        // Weights multiply: a +1 meeting a −1 contributes −1.
                        out.try_add_row(row, lw.checked_mul(*rw).ok_or("join weight overflow")?)
                            .map_err(arithmetic)?;
                    }
                }
            }
            Ok(out)
        }

        IncNodeType::Union => {
            let l = eval(kids[0], ctx)?;
            let r = eval(kids[1], ctx)?;
            l.try_union(&r).map_err(arithmetic)
        }

        IncNodeType::Negate => eval(kids[0], ctx)?.try_negate().map_err(arithmetic),

        IncNodeType::Agg { groups, aggs } => {
            let input = eval(kids[0], ctx)?;
            aggregate(&input, groups, aggs)
        }

        IncNodeType::MergeAgg { groups, aggs } => {
            // children: [partial-of-delta, stored-state]
            let partial = eval(kids[0], ctx)?;
            let stored = eval(kids[1], ctx)?;
            merge_agg(&partial, &stored, groups, aggs)
        }
    }
}

/// Number of accumulator columns: one count plus one slot per `Sum`.
pub fn accumulators(aggs: &[AggKind]) -> usize {
    1 + aggs.iter().filter(|a| matches!(a, AggKind::Sum(_))).count()
}

/// Group `input` and emit, per group: the group keys followed by the accumulator
/// columns (`count`, then one `sum` per `AggKind::Sum`).
///
/// The output is the aggregate's *intermediate state*, not a finalised value —
/// that is what lets a later batch merge into it (`agg.md` §1).
///
/// `SUM` only accepts integers. The feed maps decimal, float and binary columns
/// to `Value::Str`, so `SUM` over one of those is not silently zero: it is an
/// error. Widening the accumulator is a separate change; until then those types
/// support identity comparison only.
pub fn aggregate(input: &Multiset, groups: &[usize], aggs: &[AggKind]) -> Result<Multiset, String> {
    // key -> accumulators
    let mut acc: BTreeMap<Row, Vec<i64>> = BTreeMap::new();
    for (row, w) in input.iter() {
        let key: Row = groups
            .iter()
            .map(|c| column(row, *c).cloned())
            .collect::<Result<_, _>>()?;
        let slot = acc
            .entry(key)
            .or_insert_with(|| vec![0; accumulators(aggs)]);
        // Accumulator 0 is always the count; each `Sum` takes the next slot in
        // order. (`Count` therefore does not consume a slot of its own — sizing
        // the layout as `aggs.len() + 1` is the mistake that made the delta and
        // the recompute disagree.)
        slot[0] = slot[0].checked_add(*w).ok_or("aggregate count overflow")?;
        let mut sum_ordinal = 0;
        for kind in aggs.iter() {
            if let AggKind::Sum(c) = kind {
                let v = column(row, *c)?.clone();
                // NULL does not contribute to SUM, matching SQL.
                match v {
                    Value::Null => {}
                    Value::Int(n) => {
                        let n = n.checked_mul(*w).ok_or("SUM multiplication overflow")?;
                        slot[1 + sum_ordinal] =
                            slot[1 + sum_ordinal].checked_add(n).ok_or("SUM overflow")?;
                    }
                    // A decimal/float/binary column arrives as `Str`, so this
                    // is reachable from a legal plan, not a programmer error:
                    // it must be reported, not asserted away. The message
                    // names the column, not the value: binary values are hex
                    // and strings can be long, and neither belongs in an error
                    // log.
                    _ => {
                        return Err(format!(
                            "SUM over column {c}: non-integer values are not summable"
                        ));
                    }
                }
                sum_ordinal += 1;
            }
        }
    }
    let mut out = Multiset::new();
    for (key, values) in acc {
        let mut row = key;
        row.extend(values.into_iter().map(Value::Int));
        out.try_add_row(row, 1).map_err(arithmetic)?;
    }
    Ok(out)
}

/// Merge a partial aggregate into stored state, joining on the group keys.
///
/// A group whose accumulated **count reaches zero is dropped** — a group that
/// merely sums to zero is not empty, which is why emptiness is tracked by the
/// count and not inferred from the value (`agg.md` §1).
pub fn merge_agg(
    partial: &Multiset,
    stored: &Multiset,
    groups: &[usize],
    aggs: &[AggKind],
) -> Result<Multiset, String> {
    let acc_width = accumulators(aggs);
    let mut acc: BTreeMap<Row, Vec<i64>> = BTreeMap::new();

    for source in [partial, stored] {
        for (row, w) in source.iter() {
            if row.len() != groups.len() + acc_width || *w != 1 {
                return Err("invalid aggregate accumulator shape or weight".into());
            }
            let key: Row = row.iter().take(groups.len()).cloned().collect();
            let vals: Vec<i64> = row
                .iter()
                .skip(groups.len())
                .map(|v| match v {
                    Value::Int(i) => Ok(*i),
                    _ => Err("non-integer aggregate accumulator".to_string()),
                })
                .collect::<Result<_, _>>()?;
            let slot = acc.entry(key).or_insert_with(|| vec![0; acc_width]);
            for (i, v) in vals.iter().enumerate() {
                if i < slot.len() {
                    slot[i] = slot[i]
                        .checked_add(
                            v.checked_mul(*w)
                                .ok_or("aggregate multiplication overflow")?,
                        )
                        .ok_or("aggregate merge overflow")?;
                }
            }
        }
    }

    let mut out = Multiset::new();
    for (key, values) in acc {
        // count is accumulator 0; a zero count means the group has no rows left.
        if values.first().copied().unwrap_or(0) < 0 {
            return Err("negative aggregate group count".into());
        }
        if values.first().copied().unwrap_or(0) == 0 {
            continue;
        }
        let mut row = key;
        row.extend(values.into_iter().map(Value::Int));
        out.try_add_row(row, 1).map_err(arithmetic)?;
    }
    Ok(out)
}

fn arithmetic(e: crate::change::ArithmeticError) -> String {
    e.to_string()
}

fn column(row: &Row, index: usize) -> Result<&Value, String> {
    row.get(index)
        .ok_or_else(|| format!("column {index} is outside row width {}", row.len()))
}

/// Three-valued-ish evaluation: `Int(1)` is TRUE, anything else is not TRUE.
fn eval_expr(expr: &Expr, row: &Row) -> Result<Value, String> {
    Ok(match expr {
        Expr::Col(c) => column(row, *c)?.clone(),
        Expr::Lit(v) => v.clone(),
        Expr::Gt(a, b) => match (eval_expr(a, row)?, eval_expr(b, row)?) {
            (Value::Int(x), Value::Int(y)) => Value::Int((x > y) as i64),
            _ => Value::Null, // NULL comparison is UNKNOWN, never TRUE
        },
        Expr::Eq(a, b) => match (eval_expr(a, row)?, eval_expr(b, row)?) {
            (Value::Int(x), Value::Int(y)) => Value::Int((x == y) as i64),
            (Value::Str(x), Value::Str(y)) => Value::Int((x == y) as i64),
            _ => Value::Null,
        },
    })
}
