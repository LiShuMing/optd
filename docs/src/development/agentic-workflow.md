# Agentic Development Workflow — Design

**Status:** proposed · 2026-10-28
**Scope:** how humans and AI agents change this repository, not how the engine works.

This document designs the working agreement that `AGENT.md` states normatively. The rule it
introduces first is the one it follows: **a change starts with a design document, and the
implementation follows the document.**

---

## 1. The problem

The repository is developed by several agents and humans in parallel. The failures we have
actually hit are process failures, not compiler failures:

| Observed failure | Cost |
| --- | --- |
| A fix lands without a written decision; the next agent re-litigates it | duplicated work, inconsistent invariants |
| A report says "verified" but the evidence was a cached build or a single happy path | false confidence, later regressions |
| Commit messages carry an internal template and a corporate email | the branch cannot be published as-is |
| Two agents edit the same lane because ownership was only in a chat message | conflicting edits, unclear blame |
| A bug fix has no regression test, so the same bug returns | silent wrongness |
| A performance claim has no hardware, command, or baseline | unreviewable numbers |

The common cause is that the rules live in individuals' memory and in chat. Chat is a good
transport and a poor specification.

## 2. Goals and non-goals

**Goals**

1. Every non-trivial change has a reviewable design artifact before code.
2. Every claim of correctness is backed by a command and an observed result.
3. Work is attributable: one owner per lane, one logical change per commit.
4. The repository can be published at any time without a cleanup pass.
5. The process is cheap enough that agents follow it under time pressure.

**Non-goals**

* Replacing code review with documents. The design is an input to review, not a substitute.
* Prescribing an agent runtime or vendor. The rules are tool-agnostic.
* Making every typo fix write a design. §4 defines the exceptions.

## 3. Principles

### 3.1 Design first, then code

A design document is the unit of agreement. It is where constraints, alternatives, and failure
modes are argued once, so the implementation can be reviewed against a stated intent.

The design does not need to be long. It needs to answer:

* What problem is being solved, and what is explicitly out of scope?
* What facts and constraints was the decision checked against?
* What alternatives were rejected, and why?
* What can fail, and what happens then?
* How will we know it works (acceptance criteria)?
* What remains open?

### 3.2 Evidence over assertion

A statement is one of three things: **verified** (a command was run, output observed),
**inferred** (reasoned from code/spec), or **unknown**. Reports label them. "Tests pass" without
the command, toolchain, and count is an assertion, not evidence.

### 3.3 Small, reversible changes

One logical change per commit. Small changes are easier to review, bisect, and revert; they also
make an agent's mistake cheap.

### 3.4 One writer per lane

Concurrency without ownership produces the same edits from two directions. A task is claimed
before execution; a lane has one canonical owner; a reviewer is read-only unless asked to patch.

### 3.5 Publishable by default

No secret, internal URL, internal product name, internal email, or pasted internal source in any
artifact. Public remotes use the project's public identity. The check is mechanical where
possible (§7).

## 4. Workflow

```
idea / bug / request
        │
        ▼
[1] design doc  ──► review ──► accepted (or rejected / superseded)
        │
        ▼
[2] implementation + tests (referencing the design)
        │
        ▼
[3] verification (fresh commands, recorded output)
        │
        ▼
[4] commit / PR (one logical change, public identity)
        │
        ▼
[5] review + merge (branch, never direct to main)
```

### 4.1 When a design document is required

Required for: new features, API or schema changes, dependency/toolchain changes, performance
work, anything touching persistence or recovery, and any change whose correct behavior is not
obvious from the diff.

Not required for: typo/comment fixes, formatting-only changes, reverts of a change that is
already broken, and dependency security patches with no API change. In those cases the commit
message states the exception and the reason.

### 4.2 Where documents live

* Area designs: `docs/src/<area>/`, linked from `docs/src/SUMMARY.md`.
* Cross-cutting process designs: `docs/src/development/`.
* Every design has a status line: `proposed`, `accepted`, `superseded`, `rejected`, with a date.
* A superseded document links to its replacement; it is not deleted.

### 4.3 The template

```markdown
# <Title>

**Status:** proposed · <date>
**Scope:** <what this covers and what it does not>

## 1. Problem
## 2. Constraints and evidence
## 3. Options considered
## 4. Decision
## 5. Failure modes and recovery
## 6. Acceptance criteria
## 7. Rollout / migration
## 8. Open questions
```

The template is intentionally short. A design that cannot fit it is usually two designs.

## 5. Verification standard

Every change records, in the PR or task:

1. the exact command(s) run;
2. the toolchain and relevant environment (e.g. rustc version, compiler);
3. the observed result (counts, exit status, output excerpt);
4. what was **not** covered.

Rules that follow from past failures:

* A cached build is not evidence for a gate. Use a fresh target directory when the claim is
  "this configuration builds".
* A single happy-path test is not a correctness claim for an algebraic change. Use a
  differential/oracle test and a regression test for each fixed bug.
* A performance claim needs hardware, baseline commit, command, and before/after numbers.
* A report distinguishes verified from inferred in its own words.

## 6. Repository conventions

* **Commits:** one logical change; imperative subject; body states what and why; no internal
  template or corporate email.
* **Branches:** work on a branch; never commit directly to `main`; no force-push to shared
  branches.
* **Dependencies:** a dependency or lockfile change is its own commit with a manifest of what
  changed and why it was forced.
* **Docs:** every design is in the book index; every public API has a doc comment.
* **Fixtures:** check in the generator, not generated data. Generated artifacts are reproducible
  from a documented command.

## 7. Making the project more agentic

Agentic does not mean "more automation". It means **the environment tells the agent what is
true and what to do next without a human relaying it**.

| Capability | Why it helps agents | Phase |
| --- | --- | --- |
| `AGENT.md` at the root | a single contract every agent reads first | 1 (this change) |
| One verification command (`scripts/agent-check.sh`) | removes guesswork about which gates matter | 2 |
| CI: fmt, clippy, test, docs, MSRV | makes the contract enforceable, not advisory | 2 |
| Design-doc template + index | gives a predictable place to start and review | 1 |
| Issue/task template with acceptance criteria | turns a request into a checkable contract | 2 |
| Secret / internal-metadata pre-commit check | prevents the cleanup pass before publishing | 3 |
| Reproducible fixtures + `#[ignore]` integration tests | expensive tests stay runnable on demand | 3 |
| Structured, stable command output | lets an agent parse results instead of scraping prose | 3 |

Deliberately not proposed: an autonomous merge bot. Review remains human-gated; the workflow
optimizes the path *to* review, not around it.

## 8. Failure modes of this process

| Failure | Symptom | Mitigation |
| --- | --- | --- |
| Design theater | code written first, document back-filled | review checks the document's decision against the diff; document date precedes the commit |
| Process overhead | trivial changes stall for a document | §4.1 exception list; short template |
| Stale documents | docs contradict code | status line + supersession links; reviewers update the doc in the same change |
| Evidence inflation | "verified" without output | §5 requires command + observed result in the report |
| Lane collisions | two agents edit one file | claim before execution; one writer per lane |
| Process drift | rules live only in AGENT.md but are not enforced | phase-2 CI and phase-3 hooks |

## 9. Acceptance criteria

1. `AGENT.md` exists at the repository root and states the design-first rule, evidence standard,
   coding conventions, and collaboration rules.
2. This document is linked from `docs/src/SUMMARY.md`.
3. A design-template section exists and is referenced by `AGENT.md`.
4. The rules contain no internal names, URLs, or credentials.
5. The change is committed with the project's public identity.

## 10. Open questions

1. Should `AGENT.md` be mirrored/symlinked as `AGENTS.md` for tools that expect that name?
2. How much of §7 phase 2 is in scope now: a shell script, a CI workflow, or both?
3. Do performance designs live with the area docs or in a separate benchmark book?
4. What is the retention/expiry policy for superseded design documents?