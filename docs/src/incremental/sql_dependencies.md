# Typed SQL Dependency Scaffold

**Status:** accepted after independent agent review · 2026-10-29
**Scope:** a buildable workspace package and lockfile entry; execution is a
separate implementation increment. No dependency version or MSRV is raised.

## 1. Problem and constraints

The reference incremental crate models only NULL, integers and strings. Typed
benchmark SQL requires the already locked DataFusion 55 and Arrow 59 stack.
Routing decimals through the legacy optimizer bridge would lose precision.
The workspace contract requires dependency changes in a separate commit.

## 2. Decision and alternatives

Add the `optd-incremental-sql` workspace package with an initially empty library.
Reuse existing DataFusion 55.0.0, serde, serde_json, sha2, thiserror, tempfile and
tokio versions. Enable DataFusion's SQL and builtin expression features needed
for date, string, regex, Unicode, nested and numeric benchmark expressions.
The manifest declares Rust 1.95, matching the existing incremental package.
No additional third-party package, version, source or checksum changes.

Extending the limited reference value enum or using floating point for decimal
arithmetic is rejected. No runtime behavior is added by this scaffold.

## 3. Failure modes and recovery

`--locked` must reject an incomplete dependency manifest. An unavailable MSRV
toolchain is a failed gate, not grounds to silently increase the version.
The scaffold can be reverted independently before consumers use its API.

## 4. Acceptance and rollout

Review the package manifest and lockfile diff, and build the exact staged tree
with Rust 1.95.0. The lockfile change adds only the new local package and its
seven dependency edges; all existing packages retain their identities.
Subsequent accepted execution designs supply the public APIs, tests and CLI.
This document does not claim those features are implemented or qualified.
