//! Observed maintenance work, separate from planning and execution.

use crate::planner::{KeyScope, RuleIdentity, Strategy};
use serde::Serialize;

/// Work observed for one operator, separate from performance claims.
#[derive(Clone, Debug, Serialize)]
pub struct NodeStats {
    /// Stable postorder identity within this query definition.
    pub id: usize,
    /// Logical operator description.
    pub operator: String,
    /// Actual refresh path.
    pub strategy: Strategy,
    /// Rule whose compiled expression was executed for this operator.
    pub rule: RuleIdentity,
    /// Number of changed distinct output rows.
    pub delta_rows: usize,
    /// Indexed aggregate work, present only for changed-group maintenance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_work: Option<GroupWork>,
    /// Actual affected-bucket snapshot work, excluding full cache/source I/O.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyed_work: Option<KeyedWork>,
}

/// Actual input work at an indexed aggregate, independent of elapsed time.
#[derive(Clone, Debug, Serialize)]
pub struct GroupWork {
    /// Number of installed groups before this refresh.
    pub total_groups_before: usize,
    /// Number of installed groups after this refresh.
    pub total_groups_after: usize,
    /// Keys whose complete input multiset changed, including removed groups.
    pub touched_groups: usize,
    /// Expanded insertions plus retractions supplied by the child delta.
    pub changed_input_rows: usize,
    /// Surviving rows actually read by the local aggregate.
    pub aggregate_input_rows: usize,
    /// Rows produced by recomputing affected groups.
    pub aggregate_output_rows: usize,
}

/// Observed operator-local indexed work; not an end-to-end complexity claim.
#[derive(Clone, Debug, Serialize)]
pub struct KeyedWork {
    /// Closure declared by the selected expression.
    pub scope: KeyScope,
    /// Installed input bucket counts before this refresh, one per port.
    pub total_keys_before: Vec<usize>,
    /// Candidate input bucket counts, one per port.
    pub total_keys_after: Vec<usize>,
    /// Union of changed keys across the input ports.
    pub touched_keys: usize,
    /// Expanded insertions and retractions received from children.
    pub changed_input_rows: usize,
    /// Rows supplied per port to a before operator evaluation.
    pub snapshot_input_rows_before: Vec<usize>,
    /// Rows supplied per port to an after operator evaluation.
    pub snapshot_input_rows_after: Vec<usize>,
    /// Actual before operator calls; windows use cached output instead.
    pub snapshot_evaluations_before: usize,
    /// Actual after operator calls.
    pub snapshot_evaluations_after: usize,
    /// Committed window output rows read for the before snapshot.
    pub cached_before_output_rows: usize,
    /// Complete child scans used to find affected buckets (zero on this path).
    /// All buckets may still be affected; source and result I/O are excluded.
    pub full_input_scans: usize,
}

/// Actual dirty nodes and source epoch for a prepared refresh.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RefreshStats {
    /// Candidate source epoch.
    pub epoch: u64,
    /// Operators that were evaluated; unchanged ancestors are omitted.
    pub nodes: Vec<NodeStats>,
}
