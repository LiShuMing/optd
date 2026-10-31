// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Producer checkpoints and exact predecessor/identity validation.

use super::canonical;
use super::error::CdcError;
use super::generation::Generation;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Version of the canonical change table and its summary envelope.
pub const PROTOCOL: &str = "optd-cdc/1";

/// Durable identity of an exact input snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCheckpoint {
    /// Stable table UUID.
    pub table_id: String,
    /// Branch name.
    pub reference: String,
    /// Snapshot id; NULL only for no snapshot.
    pub snapshot_id: Option<i64>,
    /// Snapshot sequence; NULL only for no snapshot.
    pub sequence_number: Option<i64>,
    /// Fingerprint of the source schema descriptor.
    pub schema_fingerprint: String,
}
fn checkpoints(points: &[SourceCheckpoint]) -> Result<(), CdcError> {
    // M1 deliberately refuses to claim cross-table consistency.
    if points.len() != 1 {
        return Err(CdcError::InvalidCheckpoint);
    }
    for p in points {
        if !canonical::uuid(&p.table_id)
            || p.reference != "main"
            || !canonical::hash(&p.schema_fingerprint)
            || p.snapshot_id.is_some() != p.sequence_number.is_some()
            || p.sequence_number.is_some_and(|s| s < 0)
        {
            return Err(CdcError::InvalidCheckpoint);
        }
    }
    Ok(())
}

/// Complete producer checkpoint stored in each committed snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitEnvelope {
    /// View UUID.
    pub view_id: String,
    /// Generation UUID.
    pub generation_id: String,
    /// Contiguous logical sequence, independent of Iceberg's assigned sequence.
    pub batch_seq: i64,
    /// Predecessor identity, absent only for bootstrap.
    pub previous_batch_id: Option<String>,
    /// SHA-256 batch identity.
    pub batch_id: String,
    /// Bound view descriptor digest.
    pub plan_fingerprint: String,
    /// Bound output schema digest.
    pub schema_fingerprint: String,
    /// Unkeyed in M1.
    pub profile: String,
    /// Value representation version.
    pub payload_representation: String,
    /// Exact predecessor input snapshots, absent for full bootstrap.
    pub source_from: Option<Vec<SourceCheckpoint>>,
    /// Exact target snapshots from which to recover the view.
    pub source_to: Vec<SourceCheckpoint>,
    /// Bootstrap or delta.
    pub kind: String,
    /// Physical row count, including zero.
    pub record_count: i64,
    /// Canonical logical payload digest.
    pub delta_hash: String,
    /// UTC microseconds when candidate was prepared.
    pub prepared_at_us: i64,
    /// Diagnostic owner, not a fence.
    pub writer_id: String,
}
impl CommitEnvelope {
    pub(super) fn identity(&self) -> Result<String, CdcError> {
        canonical::fingerprint(&json!([
            "optd-batch/1",
            self.view_id,
            self.generation_id,
            self.batch_seq,
            self.previous_batch_id,
            self.plan_fingerprint,
            self.schema_fingerprint,
            self.source_from,
            self.source_to,
            self.kind
        ]))
    }
    /// Parse a persisted envelope with strict JSON and recompute identity.
    pub fn from_json(input: &str) -> Result<Self, CdcError> {
        let envelope: Self = serde_json::from_value(canonical::parse_unique_json(input)?)?;
        envelope.validate()?;
        Ok(envelope)
    }
    /// Check structure and deterministic identity before trusting a checkpoint.
    pub fn validate(&self) -> Result<(), CdcError> {
        if !canonical::uuid(&self.view_id)
            || !canonical::uuid(&self.generation_id)
            || self.batch_seq < 1
            || self.record_count < 0
            || !canonical::hash(&self.plan_fingerprint)
            || !canonical::hash(&self.schema_fingerprint)
            || !canonical::hash(&self.delta_hash)
            || self.writer_id.is_empty()
        {
            return Err(CdcError::InvalidEnvelope);
        }
        if !["unkeyed", "keyed"].contains(&self.profile.as_str())
            || self.payload_representation != "value-v1"
        {
            return Err(CdcError::UnsupportedProfile);
        }
        checkpoints(&self.source_to)?;
        match (
            &self.source_from,
            &self.previous_batch_id,
            self.kind.as_str(),
            self.batch_seq,
        ) {
            (None, None, "bootstrap", 1) => {}
            (Some(from), Some(prev), "delta", seq) if seq > 1 && canonical::hash(prev) => {
                checkpoints(from)?;
                let (a, b) = (&from[0], &self.source_to[0]);
                if a.table_id != b.table_id
                    || a.reference != b.reference
                    || a.schema_fingerprint != b.schema_fingerprint
                    || b.sequence_number <= a.sequence_number
                    || b.snapshot_id == a.snapshot_id
                {
                    return Err(CdcError::InvalidCheckpoint);
                }
            }
            _ => return Err(CdcError::InvalidEnvelope),
        }
        if self.identity()? != self.batch_id {
            return Err(CdcError::DigestMismatch);
        }
        Ok(())
    }
    /// Check the immutable table-generation bindings.
    pub fn validate_generation(&self, generation: &Generation) -> Result<(), CdcError> {
        self.validate()?;
        let props = generation.properties()?;
        if self.view_id != generation.view_id()
            || self.generation_id != generation.generation_id()
            || self.plan_fingerprint != props["optd.plan_fingerprint"]
            || self.schema_fingerprint != props["optd.schema_fingerprint"]
            || self.profile != generation.schema().profile()
        {
            return Err(CdcError::InvalidCheckpoint);
        }
        let binding = generation
            .bindings()
            .values()
            .next()
            .ok_or(CdcError::InvalidCheckpoint)?;
        if self.source_to[0].table_id != binding.table_id
            || self.source_to[0].reference != binding.reference
        {
            return Err(CdcError::InvalidCheckpoint);
        }
        Ok(())
    }
    /// Enforce exactly one predecessor and contiguous batch sequence.
    pub fn follows(&self, previous: Option<&Self>) -> Result<(), CdcError> {
        self.validate()?;
        match previous {
            None if self.batch_seq == 1 => Ok(()),
            Some(prev)
                if self.previous_batch_id.as_ref() == Some(&prev.batch_id)
                    && self.batch_seq
                        == prev.batch_seq.checked_add(1).ok_or(CdcError::Overflow)?
                    && self.source_from.as_ref() == Some(&prev.source_to)
                    && self.view_id == prev.view_id
                    && self.generation_id == prev.generation_id
                    && self.plan_fingerprint == prev.plan_fingerprint
                    && self.schema_fingerprint == prev.schema_fingerprint =>
            {
                Ok(())
            }
            _ => Err(CdcError::Conflict),
        }
    }
}
