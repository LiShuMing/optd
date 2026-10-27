// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The refresh driver: LOAD then CATCH-UP, with the guards the design requires.
//!
//! Three guards are implemented here rather than left to callers, because each of
//! them corresponds to a way an incremental engine is *silently* wrong:
//!
//! 1. **Capability check** — a view over a mutated table needs a feed that can
//!    express deletes; a multi-table view needs a cross-table position.
//! 2. **Unresolved marker** — a delta plan with a marker the rules could not
//!    resolve would execute and produce changes-only data. It must fail instead.
//! 3. **Watermark transactionality** — the watermark advances only after the new
//!    contents have been computed, so a failure leaves the view at the previous
//!    position rather than in a half-applied state.

use std::collections::BTreeMap;

use crate::change::{Capabilities, Multiset, Position, TableId};
use crate::interp::{eval, EvalContext, Ranges};
use crate::plan::{children_of, delta, IncNodeType, IncPlanNode};
use crate::rules::{has_unresolved_delta, rewrite, RewriteError};
use crate::source::DeltaSource;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshError {
    /// The feed cannot support this view. The caller must rebuild.
    InsufficientCapabilities { missing: &'static str },
    /// The rules could not resolve a marker; rebuilding is the only correct path.
    CannotMaintain(RewriteError),
    /// The plan is evaluable but its shape is one this milestone cannot maintain
    /// soundly. Accepting it would load fine and then drift from a recompute.
    UnsupportedShape(String),
    /// The caller's table list omits a table the plan reads. A caller error, not
    /// a property of the plan; tracked separately so it can be reported as one.
    TableListMismatch { missing: Vec<TableId> },
    /// The plan is not evaluable.
    Eval(String),
}

impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshError::InsufficientCapabilities { missing } => {
                write!(f, "feed lacks `{}`: full rebuild required", missing)
            }
            RefreshError::CannotMaintain(e) => write!(f, "{}", e),
            RefreshError::UnsupportedShape(m) => write!(f, "unsupported plan shape: {}", m),
            RefreshError::TableListMismatch { missing } => write!(
                f,
                "the plan reads table(s) {:?} that the caller did not list",
                missing
            ),
            RefreshError::Eval(m) => write!(f, "evaluation failed: {}", m),
        }
    }
}

/// A maintained view.
pub struct View {
    /// The view definition, with no change marker.
    plan: IncPlanNode,
    /// `Δ(view)`, rewritten once at LOAD — the plan shape does not change between
    /// refreshes, only the data does.
    delta_plan: IncPlanNode,
    /// Base tables the view reads.
    tables: Vec<TableId>,
    /// Applied position per base table.
    watermarks: BTreeMap<TableId, Position>,
    contents: Multiset,
}

impl View {
    /// LOAD: build the view from a snapshot taken at the source's current
    /// position, and rewrite `Δ(view)` once.
    pub fn load(
        plan: IncPlanNode,
        source: &dyn DeltaSource,
        tables: Vec<TableId>,
    ) -> Result<Self, RefreshError> {
        check_capabilities(&plan, &tables, source.capabilities())?;

        // The aggregate rule merges into `StoredView`, which is *this view's*
        // contents. That is the accumulator relation only when the aggregate is
        // the root; anywhere else the stored rows are projected/filtered rows and
        // the merge would silently be against the wrong relation (`agg.md` §4 —
        // the rule is "only valid at the view root / where the state lives").
        if let Some(above) = non_root_aggregate(&plan) {
            return Err(RefreshError::UnsupportedShape(format!(
                "an aggregate below `{}` cannot be maintained: its merge state would be the view's \
                 final rows, not accumulator rows",
                above
            )));
        }

        // Track the tables the *plan* reads. Trusting the caller's list instead
        // means an omitted table is silently never refreshed (finding C), which is
        // invisible until the view drifts.
        let plan_tables = distinct_tables(&plan);
        let missing: Vec<TableId> = plan_tables
            .iter()
            .copied()
            .filter(|t| !tables.contains(t))
            .collect();
        if !missing.is_empty() {
            return Err(RefreshError::TableListMismatch { missing });
        }

        let tables = plan_tables;

        let delta_plan = rewrite(delta(plan.clone()))
            .map_err(RefreshError::CannotMaintain)?;

        let start = source.current_position();
        let watermarks = tables.iter().map(|t| (*t, start)).collect();

        // The initial contents are the plan evaluated with **no changes** in
        // range, i.e. over the snapshot alone.
        let empty_ranges: Ranges = BTreeMap::new();
        let ctx = EvalContext {
            source,
            ranges: &empty_ranges,
            stored_view: &Multiset::new(),
        };
        let contents = eval(&plan, &ctx).map_err(RefreshError::Eval)?;

        Ok(Self {
            plan,
            delta_plan,
            tables,
            watermarks,
            contents,
        })
    }

    /// CATCH-UP: apply the changes since the last refresh.
    ///
    /// Returns the delta applied, or `None` when there was nothing to do.
    pub fn refresh(&mut self, source: &dyn DeltaSource) -> Result<Option<Multiset>, RefreshError> {
        let now = source.current_position();
        let mut ranges: Ranges = BTreeMap::new();
        for t in &self.tables {
            let from = *self.watermarks.get(t).unwrap_or(&0);
            if now > from {
                ranges.insert(*t, (from, now));
            }
        }
        if ranges.is_empty() {
            return Ok(None);
        }

        // Guard: a marker left unresolved means the plan is not maintainable.
        if has_unresolved_delta(&self.delta_plan) {
            return Err(RefreshError::CannotMaintain(RewriteError::Unresolved(
                "marker not pushed to a scan".into(),
            )));
        }

        let ctx = EvalContext {
            source,
            ranges: &ranges,
            stored_view: &self.contents,
        };
        let d = eval(&self.delta_plan, &ctx).map_err(RefreshError::Eval)?;

        // Validate before committing. Assigning first and checking after would
        // leave `contents` mutated while the watermark still points at the old
        // position: the failed range would be replayed onto the corrupt state and
        // the view could never recover (finding B).
        let next = self.contents.union(&d);
        if !next.is_valid_relation() {
            return Err(RefreshError::Eval(
                "incremental maintenance produced a negative row count".into(),
            ));
        }
        // Commit: contents, then watermarks. The window between the two is not
        // recoverable by replaying: a delta is not idempotent, so re-applying the
        // same range would apply it twice (aggregates show it first). Recovery
        // from a crash at this point is a fresh LOAD, not a hot resume.
        //
        // Both statements are plain assignments with no I/O in between, so the
        // window is a panic-only one; a process crash leaves neither durable and
        // is handled by the same rule.
        self.contents = next;
        for t in &self.tables {
            self.watermarks.insert(*t, now);
        }
        Ok(Some(d))
    }

    pub fn contents(&self) -> &Multiset {
        &self.contents
    }

    pub fn watermarks(&self) -> &BTreeMap<TableId, Position> {
        &self.watermarks
    }

    /// Evaluate the view definition over the current data, ignoring all
    /// incremental state. This is the oracle the differential test compares
    /// against — an incremental engine's only meaningful correctness claim.
    pub fn recompute(&self, source: &dyn DeltaSource) -> Result<Multiset, RefreshError> {
        let empty_ranges: Ranges = BTreeMap::new();
        let ctx = EvalContext {
            source,
            ranges: &empty_ranges,
            stored_view: &Multiset::new(),
        };
        eval(&self.plan, &ctx).map_err(RefreshError::Eval)
    }
}


impl std::fmt::Debug for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("View")
            .field("contents", &self.contents)
            .field("watermarks", &self.watermarks)
            .finish_non_exhaustive()
    }
}

/// The capabilities a view definition requires.
///
/// This is the mechanical form of `change_capture.md` §7: the *shape of the view*,
/// not the storage it sits on, decides what the feed must be able to express.
fn check_capabilities(
    plan: &IncPlanNode,
    tables: &[TableId],
    caps: Capabilities,
) -> Result<(), RefreshError> {
    if !caps.ordered {
        return Err(RefreshError::InsufficientCapabilities { missing: "ordered" });
    }
    if !caps.gap_free {
        return Err(RefreshError::InsufficientCapabilities { missing: "gap_free" });
    }
    // A view over any table that can be mutated needs deletes to be representable.
    // Assuming "nothing is deleted" is exactly the silent-staleness trap of a
    // version-column feed.
    if !caps.expresses_deletes {
        return Err(RefreshError::InsufficientCapabilities {
            missing: "expresses_deletes",
        });
    }
    // More than one base table means the before/after pairing is only meaningful
    // if the positions are comparable across tables.
    if distinct_tables(plan).len() > 1 && !caps.cross_table_position {
        return Err(RefreshError::InsufficientCapabilities {
            missing: "cross_table_position",
        });
    }
    let _ = tables;
    Ok(())
}

/// If an `Agg` appears anywhere but the plan root, name the operator above it.
///
/// Only the root aggregate's output *is* the view's contents, and the merge rule
/// reads the stored state from there. Detecting this here keeps the failure at
/// LOAD time, where the caller can rebuild, instead of letting a loaded view drift.
fn non_root_aggregate(plan: &IncPlanNode) -> Option<&'static str> {
    for child in children_of(plan) {
        if matches!(child.typ, IncNodeType::Agg { .. }) {
            return Some(operator_name(&plan.typ));
        }
        if let Some(found) = non_root_aggregate(child) {
            return Some(found);
        }
    }
    None
}

fn operator_name(typ: &IncNodeType) -> &'static str {
    match typ {
        IncNodeType::Scan(_) => "Scan",
        IncNodeType::EmptyRelation => "EmptyRelation",
        IncNodeType::Projection(_) => "Projection",
        IncNodeType::Filter => "Filter",
        IncNodeType::Join(_) => "Join",
        IncNodeType::Union => "Union",
        IncNodeType::Agg { .. } => "Agg",
        IncNodeType::Negate => "Negate",
        IncNodeType::MergeAgg { .. } => "MergeAgg",
        IncNodeType::Delta => "Delta",
        IncNodeType::StoredView => "StoredView",
    }
}

/// The base tables a plan reads, deduplicated and sorted.
pub fn distinct_tables(plan: &IncPlanNode) -> Vec<TableId> {
    let mut out = Vec::new();
    collect_tables(plan, &mut out);
    out.sort_unstable();
    out.dedup();
    out
}

fn collect_tables(plan: &IncPlanNode, out: &mut Vec<TableId>) {
    if let IncNodeType::Scan(t) = plan.typ {
        out.push(t);
    }
    for c in children_of(plan) {
        collect_tables(c, out);
    }
}