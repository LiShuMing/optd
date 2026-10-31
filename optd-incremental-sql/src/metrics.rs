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
    /// Delta-only retractable aggregate work, excluding full relation caches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accumulator_work: Option<AccumulatorWork>,
}

/// Actual aggregate-state work; historical group rows are never read on this path.
#[derive(Clone, Debug, Serialize)]
pub struct AccumulatorWork {
    /// Distinct groups changed by this batch.
    pub touched_groups: usize,
    /// Insertions plus retractions supplied to accumulators.
    pub delta_input_rows: usize,
    /// Historical input rows revisited by aggregate evaluation.
    pub historical_input_rows: usize,
    /// Installed aggregate groups before the batch.
    pub groups_before: usize,
    /// Candidate aggregate groups after the batch.
    pub groups_after: usize,
    /// Actual indexed MIN/MAX work; absent for native-only nodes and older reports.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_index_work: Option<ValueIndexWork>,
    /// Actual DISTINCT function membership work; absent for ordinary/older nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct_work: Option<DistinctWork>,
}

/// Operator-local ordered-value maintenance, summed across touched groups/functions.
/// These counters do not include source/cache/SQL/checkpoint work or temporary buffers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ValueIndexWork {
    /// Argument rows consumed by indexed functions, including NULLs and both signs.
    pub input_rows: usize,
    /// Nonzero consolidated values looked up and updated once per group/function.
    pub value_updates: usize,
    /// Existing persistent directory nodes actually cloned by value updates.
    pub directory_entries_copied: usize,
    /// First/last entry lookups, including empty value indexes.
    pub extremum_lookups: usize,
}

/// DISTINCT argument counts and actual inputs supplied to native accumulators.
/// Normalized key-only DISTINCT uses group row counts and reports AccumulatorWork.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DistinctWork {
    /// Argument rows consumed by DISTINCT functions, including NULL and both signs.
    pub input_rows: usize,
    /// Nonzero net values whose retained reference counts were probed and updated.
    pub value_updates: usize,
    /// Existing persistent value-directory nodes actually cloned.
    pub directory_entries_copied: usize,
    /// Non-NULL values whose count first became positive, supplied once to native state.
    pub entering_values: usize,
    /// Non-NULL values whose final occurrence disappeared, retracted once from native state.
    pub leaving_values: usize,
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
    /// Full relation-cache rebuilds and state-directory copies during prepare.
    /// This does not include SQL DML, external-source IO, checkpoints or recovery.
    pub work: crate::RefreshWork,
}
