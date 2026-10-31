# Inspect and Extend Incremental Plans

> **Current status (2026-10-31):** This document records its original design and verification checkpoint. See the [current project status](project_status_20261031.md) for implemented capabilities, open gates, and the owner-authorized publication scope. Historical result counts and local-only notes below retain their original meaning.

The typed SQL backend compiles its optimized logical graph into an executable
maintenance plan. Each graph node selects one rule from an
`IncrementalOptimizer`; the selected replacement controls refresh execution.
See [Rule-based Incremental Plan Rewriting](rule_rewrite.md) for the design and
[Typed SQL Incremental Execution](sql_execution.md) for the transaction contract.

This API makes maintenance planning extensible. It does not establish a
performance improvement. The backend still retains complete source and operator
relations, and full endpoint acquisition or explicit local recomputation remains
real work.

## Inspect the selected plan

`SqlView::new` uses the default registry. Inspect the actual compiled plan with
`view.incremental_plan().explain()` before submitting changes:

```rust
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use optd_incremental_sql::{Result, SqlView};

async fn inspect() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let initial = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    let view = SqlView::new(
        "SELECT id FROM items WHERE id > 1",
        BTreeMap::from([("items".to_owned(), initial)]),
    )
    .await?;
    println!("{}", view.incremental_plan().explain());
    Ok(())
}
```

The explanation includes graph input identities, rule name/version/priority,
maintenance strategy, executable expression tree and selection reason. These
are diagnostics for the compiled plan. A rule name or a `NativeDelta` strategy
alone does not measure the work or prove result correctness. Refresh statistics
report the selected rule and the actual maintenance work.

`DurableSqlView` and `IcebergSqlView` expose their typed SQL view through
`view()`, so inspect them with
`view.view().incremental_plan().explain()`.

## Inspect and transform the Optd plans

`view.incremental_plan().programs()` exposes the actual `CompiledProgram` for
each original relation. `logical` and `physical` are
`optd_core::nodes::ArcPlanNode<IncrementalNode>`; `steps` are the verified
physical instructions, with repeated subtrees sharing one execution. `state`
declares the typed index independently of runtime SQL classification.

Register an ordinary Optd
`Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>>` using
`IncrementalOptimizer::with_plan_rule(Arc::new(rule))`. These logical rules run
after scoped-snapshot expansion and before physical implementation. Their
results must be materialized, typed, bounded logical trees with valid versioned
bindings and state wiring. They never run during refresh. The existing
`IncrementalRule` API below is retained as a compatibility frontend and compiled
into the same plans; its `DeltaExpr` is no longer interpreted by the executor.

An outer join's actual plan contains `ProjectKeys`, one shared `PrepareIndex`,
Before/After `IndexLookup` operations, `EvaluateSnapshot`, signed `Sum/Negate`
and `CompleteIndex`. Window Before uses `CachedOutput`. Group maintenance has
separate preparation, lookup, aggregate application and completion operations.
Self-contained `RelationOperator` templates retain qualified SQL expressions and
schema-only input ports, without the original table providers.

See [the accepted architecture](optd_plan_execution.md) and
`tests/optd_plans.rs` for an executable Optd rule that replaces the two-term
inner-join formula with three terms and checks its signed result against
independent full SQL.

## Default rules

The default optimizer includes explicit rule families for:

* source-independent constants;
* ordinary scans applying signed source changes through pushed scan expressions;
* linear operators, such as projection, filter, alias and union;
* inner joins using both input changes and the appropriate before/after states;
* qualified grouped aggregation, including normalized DISTINCT, using changed-group maintenance;
* qualified outer/semi/anti equijoins using indexed affected-key snapshots;
* qualified partitioned windows using indexed affected partitions and committed output;
* fetch scans, ordered chains and supported nonlinear operators using explicit
  local recomputation.

It has no universal rule that silently runs an unsupported whole query. A plan
without a matching rule fails construction. Compatible grouped aggregation
reports `ChangedGroupRecompute`; global or unqualified aggregation can report
`OperatorRecompute`. DataFusion can lower SQL constructs into another logical
operator, so read the compiled plan when determining the selected behavior.

Rules are considered by descending priority, then ascending stable name. Names
must be nonempty and unique. The first matching rule supplies the executable
replacement; a matching or rewriting error fails planning instead of trying
another implementation.

## Register a higher-priority rule

This example selects explicit local recomputation for unordered projections.
It overrides the default linear projection rule without modifying the refresh
driver. It demonstrates execution control; it does not claim that recomputing
a projection is faster.

```rust
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use optd_incremental_sql::planner::{
    IncrementalOptimizer, IncrementalRule, Maintenance, RuleContext, RuleRewrite,
};
use optd_incremental_sql::{Result, SqlView};

struct RecomputeProjection;

impl IncrementalRule for RecomputeProjection {
    fn name(&self) -> &str {
        "application.recompute_projection"
    }

    fn version(&self) -> u32 {
        1
    }

    fn priority(&self) -> i32 {
        10_000
    }

    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        Ok(matches!(context.logical_plan(), LogicalPlan::Projection(_)) && !context.ordered())
    }

    fn rewrite(&self, _context: &RuleContext<'_>) -> Result<RuleRewrite> {
        Ok(RuleRewrite {
            maintenance: Maintenance::Recompute,
            reason: "application requests local projection recomputation".to_owned(),
        })
    }
}

async fn create_with_custom_rule(tables: BTreeMap<String, RecordBatch>) -> Result<SqlView> {
    let optimizer = IncrementalOptimizer::default().with_rule(Arc::new(RecomputeProjection))?;
    let view =
        SqlView::new_with_optimizer("SELECT id + 1 AS next_id FROM items", tables, optimizer)
            .await?;
    println!("{}", view.incremental_plan().explain());
    Ok(view)
}
```

The supplied tables must include `items` with an `id` column compatible with the
expression. Priority `10_000` is higher than the built-in rules. The explanation
will identify `application.recompute_projection` at the matching projection.
All other nodes continue through the rest of the registry. The rule reads its
planning context and returns an immutable replacement; it cannot mutate source
or checkpoint state.

For a different signed-change algorithm, a rule can instead return
`Maintenance::Delta` with checked `DeltaExpr` primitives. New stateful algorithms
that cannot be expressed by those primitives need a separately implemented and
verified interpreter primitive. Merely naming a new strategy does not provide
an executable algorithm.

## Preserve the registry across restart

Pass a caller-selected registry to `SqlView::new_with_optimizer` or
`DurableSqlView::open_with_optimizer`. The optimizer is the final argument:

```rust,ignore
let view = SqlView::new_with_optimizer(sql, tables, optimizer.clone()).await?;
println!("{}", view.incremental_plan().explain());

let view = DurableSqlView::open_with_optimizer(
    &store,
    sql,
    initial_tables,
    optimizer.clone(),
)
.await?;
```

The durable entry point either bootstraps an empty store or restores its
committed state. Release the existing owner before reopening the same store.
On restart, rebuild the same custom registry and supply it again. Checkpoints
store SQL, source state, output and delta; they do not persist custom rule code
or bind the registry's rule identities. Recovery rebuilds the plan and verifies
the committed output. A successful output check does not certify the same
rule identity as a previous process or prove that a custom rule is correct for
future changes.

The source-aware constructors are available with the `iceberg` feature:

```rust,ignore
let view = IcebergSqlView::bootstrap_with_optimizer(
    &store,
    sql,
    sources,
    initial_tuple,
    optimizer.clone(),
)
.await?;
drop(view);

let view = IcebergSqlView::reopen_with_optimizer(
    &store,
    sql,
    reopened_sources,
    optimizer,
)
.await?;
println!("{}", view.view().incremental_plan().explain());
```

All original constructors retain default-registry behavior. Supplying rules
does not change snapshot selection, source identity validation, prepare,
publication or acknowledgement. Follow the complete
[pinned Iceberg source guide](typed_iceberg_loop.md) for source construction,
explicit snapshot tuples and the refresh/reopen loop.

## Verify an extension

The planner checks local input references, schemas, arity, legal execution
primitives, ordering requirements and grouped-maintenance eligibility before
SQL graph bootstrap execution. A malformed replacement fails construction.

A structurally valid replacement can still implement the wrong algebra.
Extension authors must compare complete results and signed deltas against an
independent full-query oracle across multiple rounds. Include simultaneous
changes on both join sides, deletions, duplicates, NULL values, self joins and
reopen followed by another refresh where applicable. Explain output establishes
which expression was selected; independent comparison establishes the tested
semantics. Performance requires a separately controlled measurement.

## Key-scoped snapshots and strict planning

The [scoped-snapshot design](scoped_snapshots.md) adds
`DeltaExpr::ScopedSnapshot { scope, version }`. Unlike `InputSnapshot`, this
returns the **original operator output** over complete affected input buckets.
The selected expression declares its derived index through
`KeyScope::JoinKeys` or `KeyScope::WindowPartitions`. Built-in rules emit:

```text
Sum([
  ScopedSnapshot(scope, After),
  Negate(ScopedSnapshot(scope, Before))
])
```

Join scope unions the changed keys from both sides, including removed and new
keys. Window scope requires a common nonempty PARTITION BY; Before reads the
actual committed partition output, preserving retraction identity for ties.
After executes the original function, ordering and frame on complete affected
partitions. Each requested version is evaluated at most once and shared by
repeated references. Unsupported key equality, non-equijoins and global windows
retain explicit complete-node recomputation in compatibility mode.

Use the strict registry when a plan must reject that compatibility path:

```rust
use std::collections::BTreeMap;
use datafusion::arrow::record_batch::RecordBatch;
use optd_incremental_sql::{Result, SqlView};
use optd_incremental_sql::planner::IncrementalOptimizer;

async fn create_strict(sql: &str, tables: BTreeMap<String, RecordBatch>) -> Result<SqlView> {
    SqlView::new_with_optimizer(
        sql,
        tables,
        IncrementalOptimizer::default().require_incremental(),
    ).await
}
```

Strict mode rejects any selected `Maintenance::Recompute` before bootstrap. It
accepts `ChangedGroups` and scoped snapshot differences; it does not promise
constant work per changed row. A changed window partition or join key can be
large. Global sorting, limit/fetch and unqualified aggregation may reject an
otherwise partly incremental query, so inspect the complete compiled plan.

`NodeStats.keyed_work` reports installed buckets, affected keys, changed input
rows, rows actually supplied to before/after operator calls, evaluation counts,
and cached window output rows. `full_input_scans == 0` describes indexed bucket
selection only. The implementation still copies index maps, maintains complete
operator result caches, acquires source endpoints and writes complete checkpoints.
Default inner joins still use the existing whole-side input snapshots; the new
scope primitive is also available to custom inner-join rules. No end-to-end
O(delta), production performance or complete strict TPC coverage is claimed.
See [observed qualification](scoped_snapshot_qualification.md).
