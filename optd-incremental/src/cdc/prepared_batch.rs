// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Deterministic weighted records and immutable CDC publication candidates.

use super::canonical;
use super::commit_envelope::{CommitEnvelope, SourceCheckpoint};
use super::error::CdcError;
use super::generation::Generation;
use super::output_schema::OutputSchema;
use crate::change::{Multiset, Row};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

/// One physical net-change row; batch metadata is supplied by its envelope on serialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeRecord {
    /// Full before or after image.
    pub payload: Row,
    /// 0 delete or 1 insert.
    pub op_type: i32,
    /// False in the unkeyed M1 profile.
    pub is_update: bool,
    /// Signed multiplicity.
    pub weight: i64,
    /// NULL in M1; never guessed from full-row content.
    pub row_id: Option<String>,
    /// Zero-based position in deterministic physical row order.
    pub ordinal: i64,
}
fn records(delta: &Multiset, schema: &OutputSchema) -> Result<Vec<ChangeRecord>, CdcError> {
    let mut out = Vec::new();
    for negative in [true, false] {
        for (row, weight) in delta.iter().filter(|(_, w)| (**w < 0) == negative) {
            schema.validate_row(row)?;
            if *weight == 0 || *weight == i64::MIN {
                return Err(CdcError::InvalidRow);
            }
            out.push(ChangeRecord {
                payload: row.clone(),
                op_type: i32::from(!negative),
                is_update: false,
                weight: *weight,
                row_id: None,
                ordinal: i64::try_from(out.len()).map_err(|_| CdcError::Overflow)?,
            });
        }
    }
    Ok(out)
}
fn delta_digest(rows: &[ChangeRecord]) -> Result<String, CdcError> {
    canonical::fingerprint(&json!([
        "optd-delta/1",
        rows.iter()
            .map(|r| json!([
                canonical::payload(&r.payload),
                r.op_type,
                r.is_update,
                r.weight,
                r.row_id
            ]))
            .collect::<Vec<_>>()
    ]))
}

fn endpoint_records(
    generation: &Generation,
    before: &Multiset,
    after: &Multiset,
) -> Result<Vec<ChangeRecord>, CdcError> {
    generation.schema().validate_relation(before)?;
    generation.schema().validate_relation(after)?;
    if generation.schema().key().is_none() {
        return records(
            &after
                .try_difference(before)
                .map_err(|_| CdcError::Overflow)?,
            generation.schema(),
        );
    }
    let keyed = |relation: &Multiset| -> Result<BTreeMap<Row, Row>, CdcError> {
        relation
            .iter()
            .map(|(row, _)| Ok((generation.schema().key_values(row)?, row.clone())))
            .collect()
    };
    let old = keyed(before)?;
    let new = keyed(after)?;
    let keys: BTreeSet<_> = old.keys().chain(new.keys()).collect();
    let mut out = Vec::new();
    for key in keys {
        let (a, b) = (old.get(key), new.get(key));
        if a == b {
            continue;
        }
        for (row, weight) in [(a, -1), (b, 1)] {
            if let Some(row) = row {
                out.push(ChangeRecord {
                    payload: row.clone(),
                    op_type: i32::from(weight > 0),
                    is_update: a.is_some() && b.is_some(),
                    weight,
                    row_id: Some(generation.row_id(row)?),
                    ordinal: i64::try_from(out.len()).map_err(|_| CdcError::Overflow)?,
                });
            }
        }
    }
    Ok(out)
}

/// Validated immutable batch held until a definitive commit outcome.
#[derive(Clone, Debug)]
#[must_use = "a prepared CDC batch must be emitted to advance its checkpoint"]
pub struct PreparedBatch {
    envelope: CommitEnvelope,
    rows: Vec<ChangeRecord>,
}
/// One unit-weight copy in a row-expanded vendor-shaped export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpandedRecord {
    /// Original batch ordinal, with a unit signed weight.
    pub record: ChangeRecord,
    /// Zero-based duplicate index; part of the expanded event identity.
    pub copy_ordinal: i64,
}
impl PreparedBatch {
    /// Prepare full bootstrap or the next exact range. Payload schema is checked before IO.
    pub fn new(
        generation: &Generation,
        previous: Option<&CommitEnvelope>,
        source_to: Vec<SourceCheckpoint>,
        delta: &Multiset,
        prepared_at_us: i64,
        writer_id: String,
    ) -> Result<Self, CdcError> {
        if generation.schema().key().is_some() {
            return Err(CdcError::UnsupportedProfile);
        }
        Self::build(
            generation,
            previous,
            source_to,
            records(delta, generation.schema())?,
            prepared_at_us,
            writer_id,
        )
    }
    /// Prepare from complete endpoints, enforcing unique keys even on unchanged rows.
    pub fn from_endpoints(
        generation: &Generation,
        previous: Option<&CommitEnvelope>,
        source_to: Vec<SourceCheckpoint>,
        before: &Multiset,
        after: &Multiset,
        prepared_at_us: i64,
        writer_id: String,
    ) -> Result<Self, CdcError> {
        if previous.is_none() && !before.is_empty() {
            return Err(CdcError::InvalidRow);
        }
        Self::build(
            generation,
            previous,
            source_to,
            endpoint_records(generation, before, after)?,
            prepared_at_us,
            writer_id,
        )
    }
    fn build(
        generation: &Generation,
        previous: Option<&CommitEnvelope>,
        source_to: Vec<SourceCheckpoint>,
        rows: Vec<ChangeRecord>,
        prepared_at_us: i64,
        writer_id: String,
    ) -> Result<Self, CdcError> {
        if let Some(prev) = previous {
            prev.validate_generation(generation)?;
        }
        if previous.is_none() && rows.iter().any(|r| r.weight < 0) {
            return Err(CdcError::InvalidRow);
        }
        let props = generation.properties()?;
        let mut envelope = CommitEnvelope {
            view_id: generation.view_id().to_owned(),
            generation_id: generation.generation_id().to_owned(),
            batch_seq: previous.map_or(Ok(1), |p| {
                p.batch_seq.checked_add(1).ok_or(CdcError::Overflow)
            })?,
            previous_batch_id: previous.map(|p| p.batch_id.clone()),
            batch_id: String::new(),
            plan_fingerprint: props["optd.plan_fingerprint"].clone(),
            schema_fingerprint: props["optd.schema_fingerprint"].clone(),
            profile: generation.schema().profile().into(),
            payload_representation: "value-v1".into(),
            source_from: previous.map(|p| p.source_to.clone()),
            source_to,
            kind: if previous.is_some() {
                "delta"
            } else {
                "bootstrap"
            }
            .into(),
            record_count: i64::try_from(rows.len()).map_err(|_| CdcError::Overflow)?,
            delta_hash: delta_digest(&rows)?,
            prepared_at_us,
            writer_id,
        };
        envelope.batch_id = envelope.identity()?;
        envelope.validate_generation(generation)?;
        envelope.follows(previous)?;
        Ok(Self { envelope, rows })
    }
    /// Immutable durable checkpoint.
    pub fn envelope(&self) -> &CommitEnvelope {
        &self.envelope
    }
    /// Canonically ordered physical changes.
    pub fn rows(&self) -> &[ChangeRecord] {
        &self.rows
    }
    /// Expand multiplicities only after checking a caller-supplied size bound.
    /// Failure returns no partial export and cannot advance a consumer cursor.
    pub fn expanded(&self, maximum_records: usize) -> Result<Vec<ExpandedRecord>, CdcError> {
        let mut count = 0usize;
        for r in &self.rows {
            count = count
                .checked_add(
                    usize::try_from(r.weight.unsigned_abs()).map_err(|_| CdcError::Overflow)?,
                )
                .ok_or(CdcError::Overflow)?;
            if count > maximum_records {
                return Err(CdcError::Overflow);
            }
        }
        let mut out = Vec::with_capacity(count);
        for r in &self.rows {
            for copy_ordinal in 0..r.weight.abs() {
                let mut record = r.clone();
                record.weight = r.weight.signum();
                out.push(ExpandedRecord {
                    record,
                    copy_ordinal,
                });
            }
        }
        Ok(out)
    }
    /// Validate storage-decoded rows and bind them to their summary envelope.
    pub fn from_records(
        generation: &Generation,
        envelope: CommitEnvelope,
        rows: Vec<ChangeRecord>,
    ) -> Result<Self, CdcError> {
        envelope.validate_generation(generation)?;
        if envelope.record_count != i64::try_from(rows.len()).map_err(|_| CdcError::Overflow)? {
            return Err(CdcError::InvalidRow);
        }
        let mut delta = Multiset::new();
        for (i, r) in rows.iter().enumerate() {
            if r.ordinal != i64::try_from(i).map_err(|_| CdcError::Overflow)?
                || r.weight == 0
                || r.weight == i64::MIN
                || r.op_type != i32::from(r.weight > 0)
                || (generation.schema().key().is_none() && (r.is_update || r.row_id.is_some()))
                || (generation.schema().key().is_some()
                    && (r.weight.abs() != 1
                        || r.row_id.as_ref() != Some(&generation.row_id(&r.payload)?)))
                || (envelope.kind == "bootstrap" && r.weight < 0)
            {
                return Err(CdcError::InvalidRow);
            }
            generation.schema().validate_row(&r.payload)?;
            delta
                .try_add_row(r.payload.clone(), r.weight)
                .map_err(|_| CdcError::Overflow)?;
        }
        let expected = if generation.schema().key().is_some() {
            let mut before = Multiset::new();
            let mut after = Multiset::new();
            for r in &rows {
                let endpoint = if r.weight < 0 {
                    &mut before
                } else {
                    &mut after
                };
                endpoint
                    .try_add_row(r.payload.clone(), 1)
                    .map_err(|_| CdcError::Overflow)?;
            }
            endpoint_records(generation, &before, &after)?
        } else {
            records(&delta, generation.schema())?
        };
        if expected != rows {
            return Err(CdcError::InvalidRow);
        }
        if delta_digest(&rows)? != envelope.delta_hash {
            return Err(CdcError::DigestMismatch);
        }
        Ok(Self { envelope, rows })
    }
    /// Validate annotations and apply against the complete predecessor relation.
    pub fn apply_to(
        &self,
        generation: &Generation,
        before: &Multiset,
    ) -> Result<Multiset, CdcError> {
        self.envelope.validate_generation(generation)?;
        let after = before
            .try_union(&self.delta()?)
            .map_err(|_| CdcError::Overflow)?;
        if endpoint_records(generation, before, &after)? != self.rows {
            return Err(CdcError::InvalidRow);
        }
        Ok(after)
    }
    /// Recover the weighted relation change, without treating log records as ordinary rows.
    pub fn delta(&self) -> Result<Multiset, CdcError> {
        let mut out = Multiset::new();
        for r in &self.rows {
            out.try_add_row(r.payload.clone(), r.weight)
                .map_err(|_| CdcError::Overflow)?;
        }
        Ok(out)
    }
}
