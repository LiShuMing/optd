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
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Prepared refresh belongs to another view or no longer follows this state.
    StaleCandidate,
    /// A requested source position moved backwards.
    InvalidPosition,
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
            RefreshError::StaleCandidate => {
                write!(f, "prepared refresh is stale or belongs to another view")
            }
            RefreshError::InvalidPosition => {
                write!(f, "source position moved backwards or is out of range")
            }
        }
    }
}

impl std::error::Error for RefreshError {}

static NEXT_VIEW: AtomicU64 = AtomicU64::new(1);

/// An immutable refresh result; the driver commits output before acknowledging it.
#[derive(Debug)]
#[must_use = "a prepared refresh must be acknowledged to advance the view"]
pub struct PreparedRefresh {
    owner: u64,
    revision: u64,
    before: BTreeMap<TableId, Position>,
    after: BTreeMap<TableId, Position>,
    contents: Multiset,
    delta: Multiset,
}
impl PreparedRefresh {
    /// Changes to publish, including an empty net change for a nonempty range.
    pub fn delta(&self) -> &Multiset {
        &self.delta
    }
    /// Pinned predecessor positions.
    pub fn from(&self) -> &BTreeMap<TableId, Position> {
        &self.before
    }
    /// Pinned target positions.
    pub fn to(&self) -> &BTreeMap<TableId, Position> {
        &self.after
    }
    /// Candidate full contents, useful for independent validation.
    pub fn contents(&self) -> &Multiset {
        &self.contents
    }
}

/// A maintained view.
pub struct View {
    owner: u64,
    revision: u64,
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
        Self::load_at(plan, source, tables, source.current_position())
    }

    /// LOAD at an exact retained snapshot, never at current state with an old watermark.
    pub fn load_at(
        plan: IncPlanNode,
        source: &dyn DeltaSource,
        tables: Vec<TableId>,
        start: Position,
    ) -> Result<Self, RefreshError> {
        if start > source.current_position() {
            return Err(RefreshError::InvalidPosition);
        }
        check_capabilities(&plan, source.capabilities())?;

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

        let delta_plan = rewrite(delta(plan.clone())).map_err(RefreshError::CannotMaintain)?;

        let watermarks = tables.iter().map(|t| (*t, start)).collect();

        // The initial contents are the plan evaluated with **no changes** in
        // range, i.e. over the snapshot alone.
        let empty_ranges: Ranges = BTreeMap::new();
        let ctx = EvalContext {
            source,
            position: start,
            ranges: &empty_ranges,
            stored_view: &Multiset::new(),
        };
        let contents = eval(&plan, &ctx).map_err(RefreshError::Eval)?;

        if !contents.is_valid_relation() {
            return Err(RefreshError::Eval("negative row count at LOAD".into()));
        }
        Ok(Self {
            owner: NEXT_VIEW.fetch_add(1, Ordering::Relaxed),
            revision: 0,
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
        let Some(candidate) = self.prepare_refresh(source)? else {
            return Ok(None);
        };
        self.acknowledge(&candidate)?;
        Ok(Some(candidate.delta))
    }

    /// Compute a refresh without changing contents or watermarks.
    pub fn prepare_refresh(
        &self,
        source: &dyn DeltaSource,
    ) -> Result<Option<PreparedRefresh>, RefreshError> {
        let now = source.current_position();
        let mut ranges: Ranges = BTreeMap::new();
        for t in &self.tables {
            let from = self.watermarks[t];
            if now < from {
                return Err(RefreshError::InvalidPosition);
            }
            if now > from {
                ranges.insert(*t, (from, now));
            }
        }
        if ranges.is_empty() {
            return Ok(None);
        }
        if has_unresolved_delta(&self.delta_plan) {
            return Err(RefreshError::CannotMaintain(RewriteError::Unresolved(
                "marker not pushed to a scan".into(),
            )));
        }
        let ctx = EvalContext {
            source,
            position: now,
            ranges: &ranges,
            stored_view: &self.contents,
        };
        let delta = eval(&self.delta_plan, &ctx).map_err(RefreshError::Eval)?;
        let contents = self
            .contents
            .try_union(&delta)
            .map_err(|e| RefreshError::Eval(e.to_string()))?;
        if !contents.is_valid_relation() {
            return Err(RefreshError::Eval(
                "incremental maintenance produced a negative row count".into(),
            ));
        }
        self.revision
            .checked_add(1)
            .ok_or(RefreshError::StaleCandidate)?;
        Ok(Some(PreparedRefresh {
            owner: self.owner,
            revision: self.revision,
            before: self.watermarks.clone(),
            after: self.tables.iter().map(|t| (*t, now)).collect(),
            contents,
            delta,
        }))
    }

    /// Acknowledge a durably committed candidate. Returns false if already acknowledged.
    pub fn acknowledge(&mut self, candidate: &PreparedRefresh) -> Result<bool, RefreshError> {
        if candidate.owner != self.owner {
            return Err(RefreshError::StaleCandidate);
        }
        if self.revision == candidate.revision + 1
            && self.watermarks == candidate.after
            && self.contents == candidate.contents
        {
            return Ok(false);
        }
        if self.revision != candidate.revision || self.watermarks != candidate.before {
            return Err(RefreshError::StaleCandidate);
        }
        self.contents = candidate.contents.clone();
        self.watermarks = candidate.after.clone();
        self.revision += 1;
        Ok(true)
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
            position: source.current_position(),
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
fn check_capabilities(plan: &IncPlanNode, caps: Capabilities) -> Result<(), RefreshError> {
    if !caps.ordered {
        return Err(RefreshError::InsufficientCapabilities { missing: "ordered" });
    }
    if !caps.gap_free {
        return Err(RefreshError::InsufficientCapabilities {
            missing: "gap_free",
        });
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
