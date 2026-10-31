# Retractable MIN/MAX value indexes

**Status:** accepted · 2026-10-31; local source/design review, implementation pending
**Scope:** R03a global/grouped builtin MIN/MAX and mixtures with already qualified
COUNT/SUM/AVG. No dependency, persisted format, source ingestion or floating
arithmetic contract change. DISTINCT, FILTER and GROUPING SETS retain their
separate roadmap gates.

## 1. Problem

`AccumulatorState::eligible` currently admits only COUNT/SUM/AVG. A grouped MIN/MAX
forces all aggregates in that node through changed-group snapshot evaluation; a
global MIN/MAX forces complete-node recomputation. Deleting the last occurrence
of an extremum needs the next surviving value, which a single cached minimum or
maximum cannot provide.

## 2. Constraints and evidence

Reuse the existing builtin accumulator rewrite, `StateSpec::Accumulator`, Optd
`UpdateAccumulator` operation and signed output delta. Rules select and declare
maintenance; the physical aggregate state executes it. Do not create an unrelated
executor or rename recomputation as delta maintenance.

Preserve exact typed schemas, duplicate multiplicities, NULL, empty global output,
group removal/recreation, immutable candidates, old Before output, checked counts,
strict refusal and current transaction/publication/recovery protocols. Existing
SUM/AVG numerical guards stay intact. TPC-H Q15 and TPC-DS Q23 use Decimal revenue
expressions and global MAX; Q23 also mixes MAX with grouped SUM. Their full SQL
loops, not synthetic COUNT-only replacements, are acceptance witnesses.

The persistent AVL `StateDirectory` already shares untouched branches and counts
actual copied nodes. Its minimum lookup is logarithmic; add a symmetric maximum
lookup, verified against the existing independent BTreeMap oracle. Do not clone a
complete BTreeMap of all values when updating one extremum.

## 3. Options considered

Recomputing the affected group's MIN/MAX is correct but rereads its entire hot
bucket. A monotonic sliding queue assumes FIFO deletion and cannot handle general
SQL DELETE/UPDATE. A counted ordered value index handles arbitrary deletion and
skips unchanged historical inputs. Select the latter, retaining the existing
native sliding accumulator states for qualified COUNT/SUM/AVG.

## 4. Decision

### Planning and types

Admit builtin MIN/MAX with exactly one immutable, non-DISTINCT argument, no FILTER,
ORDER BY or explicit null-treatment modifier, and the same builtin identity checks
as other accumulators. Initial ordered types are Null, Boolean, signed/unsigned
integers, Decimal128/256, Date32/64 and binary UTF-8 variants: the existing proven
group-key order/equality policy. Use Arrow RowConverter for one fixed typed value,
never string rendering or probabilistic hashes. Reject an encoding collision that
does not preserve exact scalar identity.

Float extrema, dictionaries, intervals, nested types, nonbinary collations and
other unqualified orders keep their explicit existing eligibility boundary.
R03e owns the separate floating contract, including NaN payloads and signed zero;
do not admit them merely because RowConverter can encode them. This staged type
boundary does not remove their remaining roadmap work.

### Physical state

Separate aggregate function metadata/cell operations into a focused child module
of `accumulator_state`. Each function specification selects exactly one mode:
native retractable state, indexed minimum, or indexed maximum. Each cell is an
enum containing either native state scalars or a counted ordered-value directory;
avoid parallel loosely synchronized optional states. Obtain empty/NULL output
types through the pinned native builtin rather than inventing result coercions.

Each non-NULL encoded value maps to its exact scalar plus a checked positive i64
reference count. Index only aggregate argument values, not historical complete
input rows. Keep group input cardinality separate: an all-NULL group exists even
when its value index is empty. Zero-row grouped entries disappear; the empty global
entry remains and emits native NULL extrema/COUNT zero.

Evaluate argument expressions on positive/negative child batches with the existing
physical expressions. Consolidate contributions by exact value within each touched
group/function, then update each nonzero value count once. Remove zero crossings;
negative final counts and arithmetic overflow fail the private candidate. Complete
row existence is still enforced by the surrounding graph/source contract, just as
for current accumulator states; projected value counts alone cannot prove it.

Read the first/last surviving directory entry to produce the new value. Compare
the group's cached complete Before output with its complete After output through
the existing exact Bag difference. A non-extremal count update can change state
without emitting output. Clone only modified index paths; discarded/stale/failed
candidates cannot mutate installed value counts or another prepared candidate.

### Work and recovery

Add optional `AccumulatorWork.value_index_work`, omitted for native-only nodes.
It reports actual argument rows consumed by indexes (including NULLs), nonzero
consolidated value updates/probes, copied directory entries and extremum lookups.
Counts sum across touched groups/functions; two independently maintained MIN/MAX
indexes count their separate work. Historical input-row visits stay zero on this
path. Existing group-directory copy work and graph/source/SQL/store work retain
their scopes; no end-to-end complexity or performance inference follows.

Old reports without the optional domain remain unknown. Numeric audit tools must
accept and sum the nested observed counters without fabricating absent values.
No derived state format changes: restart rebuilds this index from the committed
child relation through the existing bootstrap path. Bounded persisted-state
recovery remains R07b.

## 5. Failure modes and recovery

All updates belong to cloned group/cell roots. Invalid retractions, count overflow,
schema/order mismatch or expression failures return typed errors before publish.
Existing exact stale/foreign candidate and request retry behavior remains intact.
Never fall back to complete-node recomputation after admitting indexed maintenance.
An unsupported type/signature is rejected by strict planning as before.

## 6. Acceptance criteria

* RED test on the current baseline proves ordinary global/grouped MIN/MAX does
  not select AccumulatorDelta; it passes only with indexed execution installed.
* Independent SQL and integer-map oracles verify duplicate extrema, deleting one
  versus the last extremum, interior values, inserts, NULL-only/empty states,
  nullable keys, key moves, mixed aggregates and full group deletion/recreation.
* Fixed small changes with growing untouched groups and a growing hot group read
  zero historical aggregate input rows; value path copies have a logarithmic
  bound. Projection-level cancellations require no value-directory updates.
* Prepared/discarded/failed/stale candidates, multi-round randomized changes,
  durable SQL INSERT/DELETE/UPDATE, request retry, reopen and output delta replay
  agree with independent endpoints. Restart followed by deletion finds the exact
  next extremum, including duplicates and NULL.
* Every admitted type has independent native SQL/Arrow result/schema cases;
  unqualified floating arithmetic and modifier/custom builtin guards remain.
* Default/Iceberg fmt/Clippy/tests, private rustdoc, mdBook and report tools pass.
  Freeze a default binary after feature jobs and run fresh isolated TPC-H Q15 /
  TPC-DS Q23 three-round SQL, strict-plan, full-oracle, reopen/history witnesses.
  Record any additional blockers; do not mark R03a verified if these gates remain.
* Full TPC suite and controlled performance qualification remain distinct R10
  acceptance; neither is inferred from these two query witnesses.

## 7. Rollout / migration

Commit this accepted design and index link before implementation. Add the RED
strategy regression first. Implement value state and integrate through the existing
accumulator plan; preserve public import paths and additive diagnostics. Update
tests that intentionally exercised MIN/MAX through changed-group recomputation to
assert the newly executed index state and zero historical input work, retaining
their independent SQL/restart/history checks. Keep direct changed-group coverage.

## 8. Local design review

Inspected accumulator eligibility, native state creation/update, Optd rule order
and verifier, group-key type contracts, persistent directory sharing/copy counters,
group SQL/restart tests, work serialization, and the actual Q15/Q23 SQL/schema.
The critical checks are arbitrary deletion versus FIFO queues, per-group value
root sharing, exact NULL/empty types, mixed aggregate state isolation, and explicit
type/numeric boundaries. This is source/design review; implementation and query
acceptance remain unverified until recorded in ROADMAP.
