# Reusable Execution Templates (R09b)

**Status:** accepted · 2026-11-21
**Scope:** `optd-incremental-sql` physical operator execution (`executor::template`, `executor`, `engine`, `work`); excludes persistence formats, rule selection and SQL DML compilation.

## 1. Problem

Every incremental refresh invokes `executor::apply` for `Operation::Apply`, `Operation::ProjectKeys` and `Operation::EvaluateSnapshot`. Currently each invocation clones the operator's `LogicalPlan` template, walks it with `transform_up`, wraps every input `RecordBatch` in a temporary `MemTable`, runs DataFusion's `DefaultPhysicalPlanner::create_physical_plan`, collects stream partitions and concatenates output batches.

On small delta batches (such as 1–10 changed rows across a multi-operator TPC-DS graph), `ProjectKeys`, `Projection`, `Filter`, `SubqueryAlias`, `TableScan` and `Union` run twice per signed linear step (positive and negative parts) and up to four times per join step. Repeated logical-to-physical planning dominates per-refresh CPU cost even when operator states are already bounded.

## 2. Constraints and evidence

* Preserve exact typed Arrow schemas (column names, data types, nullability, metadata), SQL three-valued filter logic (`NULL` and `false` excluded), duplicate multiplicities, signed bags, candidate isolation and error propagation.
* Keep `SqlView` strictly epoch-isolated: compiled templates must never retain input batches or mutable cross-epoch state.
* Reuse pinned DataFusion 55 `PhysicalExpr` (`SessionState::create_physical_expr`) and Arrow 59 compute kernels (`filter_record_batch`, `concat_batches`). No new crate dependency.
* `RankState` and `AccumulatorState` already precompile `PhysicalExpr` once at initialization; this extends the same pattern to `executor.rs` physical programs.
* Maintain compatibility with `summarize_incremental_work.py`: every serialized diagnostic leaf must be a nonnegative integer or nested object of nonnegative integers.

## 3. Options considered

1. **Keep per-call `DefaultPhysicalPlanner`**: rejected; repeated physical planning on tiny delta batches is avoidable fixed overhead.
2. **Replace DataFusion completely with custom join/aggregate/window kernels**: rejected; duplicates complex SQL semantics and creates divergence risk against the DataFusion oracle.
3. **Compile pure stateless operators into reusable Arrow/DataFusion `PhysicalExpr` templates once per `SqlView`, with conservative fallback to `executor::apply` for stateful/complex operators**: selected.

## 4. Decision

Compile each `CompiledProgram`'s physical steps once when `SqlView` is constructed (or restored) into a parallel `CompiledProgramTemplates` table owned by `SqlView`:

| Operator shape | Compiled template | Execution kernel |
| --- | --- | --- |
| `Projection` / `ProjectKeys` (no volatile or subquery exprs) | `DirectProjection { exprs, output_schema }` | Evaluate `PhysicalExpr::evaluate(&batch)` per expression and build `RecordBatch` with exact `output_schema`. |
| `Filter` | `DirectFilter { predicate, output_schema }` | Evaluate boolean `PhysicalExpr`, reject non-boolean result, apply `arrow::compute::filter_record_batch`. |
| `TableScan` template (no `fetch`, linear filter/projection chain) | `DirectScan { filters, projection, output_schema }` | Apply compiled filters sequentially, then project column indices onto exact `output_schema`. |
| `SubqueryAlias` / `Repartition` | `DirectPassthrough { output_schema }` | Rewrap input columns with `output_schema` without cloning array buffers. |
| `Union` (same-schema bags) | `DirectUnion { output_schema }` | Direct `concat_batches` across input batches. |
| `Join`, `Aggregate`, `Window`, `Sort`, `Limit`, `Unnest`, `fetch`-bounded scans, or uncompilable expressions | `Fallback(RelationOperator)` | Existing `executor::apply` path (unchanged). |

### Empty-batch short-circuiting

For strictly row-preserving stateless templates (`DirectProjection`, `DirectFilter`, `DirectScan`, `DirectPassthrough`), an empty input `RecordBatch` (`num_rows() == 0`) produces `RecordBatch::new_empty(output_schema)` immediately without invoking expression evaluators or physical planning. Non-empty-preserving operators (such as `Aggregate` over zero rows) never take this shortcut.

### Diagnostics

Add optional `ExecutionTemplateWork` to `CacheRelationWork` (`#[serde(default, skip_serializing_if = "Option::is_none")]`) tracking:

* `direct_invocations`: executions served by precompiled direct templates;
* `fallback_invocations`: executions served by DataFusion `DefaultPhysicalPlanner`;
* `empty_short_circuits`: stateless empty-input calls resolved without evaluation.

## 5. Failure modes and recovery

* **Expression compile failure**: falls back conservatively to `Fallback(RelationOperator)` only when the operator is outside the admitted direct template shapes; invalid schemas or runtime expression errors (e.g., division by zero, overflow) return typed `Result::Err` identically to DataFusion and leave committed view state untouched.
* **Zero-column or empty batches**: `RecordBatch::try_new_with_options` preserves exact row count (`with_row_count(Some(batch.num_rows()))`) for zero-column projections so multiplicity is never lost.
* **Recovery**: templates are purely derived from `IncrementalPlan` and `SessionContext`; nothing is persisted or migrated.

## 6. Acceptance criteria

1. Differential tests comparing `CompiledOperator` direct execution against `executor::apply` across `Projection`, `Filter` (including `NULL` predicates), `TableScan` (pushed filters + projection), `SubqueryAlias`, `Union`, `ProjectKeys`, zero-row batches, zero-column projections, duplicate rows and runtime expression errors.
2. Integration regression proving `direct_invocations > 0` and `fallback_invocations == 0` for linear/key-projection refresh steps on `SqlView`, with zero output or signed-delta divergence against independent full SQL.
3. Full default + Iceberg gates (`scripts/agent-check.sh sql`), warnings-denied private rustdoc and `mdbook build docs` pass.

## 7. Rollout / migration

Internal to `SqlView` and `executor`. Additive optional `template_execution` work counters preserve compatibility with historical serialized work records. No durable format or public API breaking change.

## 8. Open questions

Extending template reuse to `Join` and `Aggregate` via dynamic epoch-slot `TableProvider` caching can be evaluated as a follow-up once stage profiling quantifies remaining `fallback_invocations` cost on SF0.01 workloads.
