// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! The `optd-cdc/1` wire contract. See `iceberg_cdc_output.md`.
//! Files and timestamps are deliberately excluded from batch identity.

mod canonical;
mod commit_envelope;
mod delta_sink;
mod error;
mod generation;
mod output_schema;
mod prepared_batch;
mod trusted_relation;

pub use canonical::{canonical_encode, fingerprint, parse_unique_json};
pub use commit_envelope::{CommitEnvelope, SourceCheckpoint, PROTOCOL};
pub use delta_sink::DeltaSink;
pub use error::CdcError;
pub use generation::{Generation, SourceBinding};
pub use output_schema::{system_fields, KeySpec, OutputField, OutputSchema, PayloadType};
pub use prepared_batch::{ChangeRecord, ExpandedRecord, PreparedBatch};
pub use trusted_relation::TrustedRelation;
