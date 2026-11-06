# Strict incremental gap execution specification

**Status:** accepted direction by owner · 2026-11-06; implementation gates below remain open.
**Scope:** typed SQL planner, Optd programs, physical maintenance, focused benchmarks.

## 1. Problem

The 2026-11-06 frozen benchmark rejected TPC-DS queries
7,9,13,17,22,26,27,35,36,39,44,49,85 in strict relational-core mode.
Small fixtures completed 86/99 strict SQL loops; generated data completed only
2/99. Plan eligibility, completed execution, result equivalence, work scope and
performance are independent gates. Original SQL retains its separate identity.

## 2. Constraints and evidence

Preserve exact typed bags and signed deltas, NULLs, duplicates, atomic candidates,
retry and recovery. Never remove a refusal guard merely to enlarge coverage.
The owner's 2026-11-06 decision permits stable STDDEV and paired floating means
with a separately declared error bound; all other comparisons retain their exact
contract. See [the numerical and rank design](strict_numeric_and_rank_design.md).
No SQL/fixture/golden substitution, dependency or durable format change. Public
SQL provenance remains in the existing manifests.

Q9 currently has fifteen Left Join blockers with no equality keys. Its right
inputs are uncorrelated scalar aggregates, sometimes behind projection/alias.
Global aggregates always emit one row, including over empty input. A filter,
grouped aggregate, LIMIT, or arbitrary scan does not prove this property.

## 3. Options considered

* Treat every no-key outer join as one global bucket: sound only with explicit
  broad work, but repeatedly evaluates unchanged pairs and is not this repair.
* Rewrite the particular Q9 text: rejected; rules must work on typed plans.
* Prove scalar cardinality and use a signed product: selected for increment S1.

## 4. Decision

Rules declare algebra and read dependencies; the executor runs closed typed
kernels. Review of S1's law and boundaries is recorded here before implementation:
for a condition-free Left Join whose right input always has exactly one row,
the output bag equals a product. Therefore
`delta = delta_left × right_before + left_after × delta_right`.
The cross term occurs once. Signed product evaluation uses an INNER product
kernel: evaluating a Left Join on negative/positive delta fragments would invent
NULL-extended rows. Preserve the original qualified output schema/nullability.

### Execution batches

| Batch | Deliverable | Gates / roadmap |
| --- | --- | --- |
| S1 | Scalar-cardinality proof, dedicated scalar-product expression/kernel, delta-guarded versioned snapshots, final physical read-dependency diagnostics and actual read counters | Q9; R06 subset only |
| S2 | Float grouping equality distinct from complete-row identity; reference-counted dedup and representative policy | Q36; NaN, signed zero, NULL, delete-last |
| S3 | Numerical contract and qualified floating aggregate state; scoped group evaluation only where independently proven | Q7/13/22/26/27/35/85; R03e |
| S4 | Stable retractable variance state and explicit sample/population behavior | Q17/39; R03d, depends on S3 |
| S5 | Ordered peer counts and affected rank ranges; common ordered-state primitives for partition windows | Q44/49; R05a/R05b, R06 ranges |
| S6 | Original SQL ordering/Top-K, source/state/checkpoint cost removal and controlled performance | R05b/R07/R08/R10; separately qualified |

S1 proves only condition-free, non-null-aware Left Join with a right global
aggregate or a projection/alias chain over one. Scalar changes legitimately
broadcast to all left rows; unchanged scalar deltas must not demand that snapshot.
Use explicit guarded reads in the Optd program, not SQL-node checks in execution.
Expose their global input IDs, Before/After versions and delta trigger IDs from
the final compiled program in preflight/benchmark output. This is a concrete
snapshot dependency contract, not completion of all R06 key/range contracts.
Record actual guarded reads, skipped reads and expanded snapshot rows separately
from existing materialization/checkpoint work. Missing older fields are unknown.

## 5. Failure modes and recovery

Cardinality proof is structural and conservative. Reject filters, grouped inputs,
matching predicates, null-aware joins and unsupported shapes. The plan verifier
checks scalar qualification and that a guarded read consumes its declared upstream
delta. Existing candidate isolation/publication remains unchanged. Recovery
rebuilds the same plan; no serialized operator state or migration is introduced.

## 6. Acceptance criteria

* Reproduce a strict refusal regression on the old implementation before S1.
* Prove positive/negative plan boundaries, including nonempty/empty scalar input,
  predicates, grouped/HAVING/filtered RHS and false cardinality assumptions.
* Compare complete rows and signed delta against full SQL and hand-derived integer
  results over inserts, deletes, UPDATE images, duplicates, NULL, two-sided changes,
  discarded candidates and invalid deletes. Verify SQL MV retry/reopen/history.
* Fixed single left change at increasing untouched left cardinality: zero left
  snapshot rows and one right snapshot row. Scalar change: read the required left
  relation and report broadcast work honestly. No full-node Replace operation.
* Default/Iceberg tests, fmt, warnings-denied Clippy; report actual scope/results.
* Rerun Q9 directed and public-generator profiles with frozen SQL/data identities,
  fresh database states and explicit deadlines. Keep baseline reports unchanged.
  Recheck the other twelve refusals. Timeouts remain incomplete, not passes.

## 7. Rollout / migration

Implement S1 first, update benchmark dependency/work reports, then record commands,
toolchain, source/data identities, result categories and remaining gates in a new
report. Never splice fresh Q9 evidence into the frozen 494-configuration baseline.
Later batches require their own detailed algorithm/numerical review before code.
No end-to-end speedup claim without a controlled paired experiment.

## 8. Open questions

S2/S3 now define signed-zero/NaN representatives and a checked exact integer AVG
domain. S4 defines stable STDDEV and paired means with the owner-approved bound.
General floating SUM/AVG and VAR_SAMP/VAR_POP remain unqualified. S5 qualifies
one global RANK expression per Window node; specialized partition/frame operators
and original-SQL Top-K remain separate gates.
General non-equality outer joins and correlated scalar cardinality are excluded
from S1. Global NULL dependencies and ordered ranges remain unfinished R06 work.
