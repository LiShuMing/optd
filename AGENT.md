# AGENT.md — Working Contract

This file is the first thing an AI agent or human contributor should read. It is normative.
The rationale and adoption plan are in
[`docs/src/development/agentic-workflow.md`](docs/src/development/agentic-workflow.md).

**Precedence:** explicit owner instruction > an accepted design document for that area > this
file > personal preference. If two rules conflict, stop and surface the conflict instead of
guessing.

---

## 1. Design before implementation

**No non-trivial change starts with code.** Write a design document first, get it reviewed, then
implement against it.

A design is required for:

* new features, APIs, schemas, or file formats;
* dependency, lockfile, toolchain, or MSRV changes;
* persistence, recovery, concurrency, or failure-handling changes;
* performance work and benchmarks;
* any change whose correctness is not obvious from the diff.

Not required for typo/comment fixes, formatting-only changes, reverts of already-broken changes,
and security patches with no API change. The commit message must state the exception.

### Where

* Area designs: `docs/src/<area>/`, linked from `docs/src/SUMMARY.md`.
* Process/cross-cutting designs: `docs/src/development/`.
* Status line: `proposed`, `accepted`, `superseded`, or `rejected`, with a date.
* Superseded documents link to their replacement; they are not deleted.

### Template

```markdown
# <Title>

**Status:** proposed · <date>
**Scope:** <covered / not covered>

## 1. Problem
## 2. Constraints and evidence
## 3. Options considered
## 4. Decision
## 5. Failure modes and recovery
## 6. Acceptance criteria
## 7. Rollout / migration
## 8. Open questions
```

The implementation's commit or PR links the design document. If implementation reveals that the
design is wrong, update the design in the same change — do not leave a document that contradicts
the code.

---

## 2. Evidence, not assertion

Label claims as **verified**, **inferred**, or **unknown**. A verification record contains:

1. the exact command(s);
2. the toolchain and relevant environment;
3. the observed result (counts, exit status, excerpt);
4. what was not covered.

Rules:

* A cached build is not evidence for "this configuration builds". Use a fresh target directory.
* One happy-path test is not correctness for an algebraic change. Use a differential/oracle test.
* Every bug fix ships a regression test that fails before the fix and passes after.
* Performance claims need hardware, baseline commit, command, and before/after numbers.
* Never mark a task done while a required check fails or was skipped.

---

## 3. Coding standards (Rust)

Use the public [Google Rust Style Guide](https://google.github.io/styleguide/rustguide.html)
as the baseline for new and substantially changed Rust code, including both incremental
crates. Apply its readability, API-design and ownership guidance in this repository's
context; this is not a claim of conformance to every Google-codebase-specific rule.

* Keep module entry points focused on declarations and explicit re-exports. Put substantial
  implementation in leaf modules named for their main type or responsibility. Separate SQL
  binding, rewrite rules, execution/state, transaction coordination and durable publication.
* Prefer descriptive names, named result/state structs and enums over positional tuples or
  ambiguous boolean parameters. Model mutually exclusive states with an enum rather than
  independent flags/options. Avoid speculative traits, generic frameworks and new crates.
* Use explicit imports; qualify free-function calls with their module when useful. Keep
  visibility as narrow as possible. Preserve existing public import paths through re-exports
  when reorganizing internals. Inline tests may use `use super::*`.
* Prefer borrowing (`&str`, `&[T]`, `&T`) to unnecessary owned copies. Retain copies required
  for isolated refresh candidates and atomic publication; explain their ownership purpose.
* Use `Option` for an expected absence and `Result` with diagnostic context for recoverable
  failure. Use pattern matching instead of check-then-unwrap, and mark uncommitted prepared
  work `#[must_use]` when silently discarding it would conceal a missing step.
* Pure refactors preserve SQL semantics, strict-plan policy, public APIs and persisted/wire
  formats. Verify both default and Iceberg feature configurations, differential semantics
  and process-recovery tests; separate any intentional behavior change into another change.

Keep conventional Cargo layout, the existing async runtime, required Arrow `Arc` sharing,
the pinned stable toolchain and compatible public types. Google rules tagged for Chromium
or Fuchsia (flat GN layouts, async restrictions or platform mutexes) are not project rules.
Do not add breaking `#[non_exhaustive]` annotations or dependencies solely for style.

* `cargo fmt` and `cargo clippy -- -D warnings` must be clean.
* The declared MSRV is a contract. Do not change it silently; changing it is a design change.
* Library code must not panic on user input. `unwrap`/`expect` require a proven invariant and a
  comment stating it. Use typed errors (`thiserror` or enums), not stringly errors.
* Public items have doc comments. Explain *why* an invariant exists, not just *what* the code does.
* Do not depend on `HashMap` iteration order for observable behavior. If order matters, sort or
  use an ordered container.
* Feature flags are additive, documented, and tested in the configurations CI actually builds.
* Tests live next to the code or in `tests/`; differential tests compare and explain both sides.
* No new dependency without an explicit reason and an owner decision when it changes the
  dependency graph materially.

---

## 4. Change discipline

* One logical change per commit; imperative subject; body states what and why.
* Work on a branch. Never commit directly to `main`. Never force-push a shared branch.
* After completing and validating a repair increment, commit it on the task branch and push it
  to `origin/main` with a normal fast-forward update. Fetch first and integrate any newer mainline
  changes before pushing. Report outstanding verification gates; never hide a failed check.
* A lockfile/dependency change is its own commit with a manifest of what moved and what forced it.
* Do not mix refactoring with behavior changes unless the refactor is required for the fix.
* Revert by reverting; do not rewrite a shared history.

### Publishability

Every commit must be publishable as-is. That means:

* no secrets, tokens, private URLs, internal product names, or pasted internal source;
* no internal/corporate email in author or committer fields — use the repository's public
  identity;
* no internal commit-message templates, issue-tracker IDs, or "generated in <internal
  environment>" footers;
* generated data is not committed; the generator and its command are.

---

## 5. Agent collaboration

* Claim a task before executing it; one canonical writer per lane.
* Do not modify another agent's lane without coordination. Reviews are read-only unless the
  reviewer is asked to patch.
* Report in this order: **outcome → evidence → caveat → next owner/action**.
* Handoffs include branch, HEAD, test status, blockers, and material locations.
* Never expose credentials, tokens, or environment contents in reports.
* Put long evidence in an attached report; keep chat to the decision and the pointer.
* If a claim fails or a message is ambiguous, say so and state the assumption; do not silently
  take over another lane.

---

## 6. Definition of done

A change is done when:

- [ ] a design document exists, is linked from the index, and matches what was built;
- [ ] the code builds and the relevant tests pass, with recorded evidence;
- [ ] `cargo fmt` and `cargo clippy -- -D warnings` are clean;
- [ ] a regression test exists for a fixed bug;
- [ ] docs and `SUMMARY.md` are updated;
- [ ] the commit is publishable (identity, secrets, internal metadata);
- [ ] the branch is pushed or the task states why it is local;
- [ ] the task status and next owner are updated.

---

## 7. Making the project more agentic

The direction is described in the workflow design (§7). In priority order:

1. This contract and the design index.
2. A single verification entry point (format, lint, tests, doc build) with stable output.
3. CI that enforces those gates, including the MSRV configuration.
4. Task/issue templates with acceptance criteria.
5. A pre-commit check for secrets, internal metadata, and non-public author identity.
6. Reproducible fixtures for expensive integration tests (`#[ignore]` + generator script).

An agent should be able to answer "what is true, what is next, and how do I prove it?" from the
repository alone.
