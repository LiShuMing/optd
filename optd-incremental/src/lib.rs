// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! # optd-incremental
//!
//! An incremental view maintenance framework built on `optd-core`.
//!
//! The design it implements is in `docs/src/incremental/`. The shape of the code
//! follows it directly:
//!
//! * [`change`] — how a change is represented (an oriented multiset, so retraction
//!   and cancellation are free).
//! * [`source`] — the change-feed contract and the capability flags that decide
//!   whether a view may be maintained at all.
//! * [`plan`] — the operator set. Defined here, rather than reused, because delta
//!   algebra needs a multiset `Union` that the host optimizer's set does not have.
//! * [`rules`] — the per-operator delta forms, and the refusal to emit a partial
//!   plan when an operator has no rule.
//! * [`engine`] — LOAD/CATCH-UP with the capability, marker and watermark guards.
//! * [`interp`] — a reference evaluator, so the rules can be *checked* against a
//!   full recompute rather than merely compiled.
//!
//! Milestone 1 deliberately covers only the maintainable core
//! (`Scan`/`Projection`/`Filter`/`Union`/inner `Join`/`Aggregate`); outer joins,
//! `Sort`/`Limit` and subqueries return [`rules::RewriteError`] and are expected
//! to be rebuilt, which is the documented policy rather than a gap.

pub mod cdc;
pub mod change;
#[cfg(feature = "iceberg")]
pub mod consumer;
pub mod engine;
#[cfg(feature = "iceberg")]
pub mod iceberg_sink;
#[cfg(feature = "iceberg")]
pub mod iceberg_source;
pub mod interp;
#[cfg(feature = "iceberg")]
pub mod local_catalog;
pub mod plan;
pub mod rules;
pub mod source;

pub use change::{Capabilities, Multiset, Position, Row, TableId, Value};
pub use engine::{PreparedRefresh, RefreshError, View};
#[cfg(feature = "iceberg")]
pub use iceberg_source::{IcebergSource, IcebergSourceError};
pub use plan::{
    agg, delta, empty, filter, join, negate, project, scan, stored_view, union, AggKind, Expr,
    IncNodeType, IncPlanNode,
};
pub use source::{DeltaSource, InMemorySource, SourceError};
