//! Iceberg-independent identities for explicitly pinned typed source tuples.
//!
//! These values describe exact endpoints. They do not establish an upstream
//! cross-table transaction or validate a snapshot's existence and ancestry.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_METADATA_DEPTH: usize = 128;

/// One complete, explicitly selected source tuple, sorted by SQL source name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedSourceVector {
    /// Metadata protocol version; the initial protocol is exactly one.
    pub version: u32,
    /// Every registered SQL source occurs exactly once.
    pub sources: BTreeMap<String, PinnedSource>,
}

/// Stable, credential-free identity of the first qualified catalog profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogBinding {
    /// Exactly `local/1` in this protocol.
    pub kind: String,
    /// Absolute canonical local catalog root, verified by the IO adapter.
    pub canonical_root: String,
}

/// A real snapshot is distinct from a table that has never had a snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PinnedSnapshot {
    /// The qualified initial table state with no snapshot.
    Empty,
    /// One retained, immutable snapshot selected by its actual identifier.
    Snapshot {
        /// Snapshot identity; its numeric order has no lineage meaning.
        id: i64,
        /// Positive Iceberg v2 sequence number.
        sequence_number: i64,
        /// Domain-separated digest of the complete immutable descriptor.
        descriptor_sha256: String,
    },
}

/// Complete fixed binding and endpoint identity for one SQL source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedSource {
    /// Qualified catalog binding; no credentials or signed URLs.
    pub catalog: CatalogBinding,
    /// Full table namespace, preserving component order.
    pub namespace: Vec<String>,
    /// Table name within the namespace.
    pub name: String,
    /// Canonical lowercase table UUID, independent of its reusable name.
    pub table_uuid: String,
    /// Exactly `main` for the initial profile.
    pub reference: String,
    /// Selected endpoint, never an instruction to follow a moving head.
    pub snapshot: PinnedSnapshot,
    /// Fixed Iceberg schema identity, including unused source columns.
    pub schema_id: i32,
    /// Complete Iceberg schema JSON, including field IDs and defaults.
    pub iceberg_schema: Value,
    /// Digest produced by [`iceberg_schema_digest`].
    pub iceberg_schema_sha256: String,
    /// Complete canonical typed Arrow layout produced by [`canonical_arrow_layout`].
    pub arrow_layout: Value,
    /// Digest produced by [`arrow_layout_digest`].
    pub arrow_layout_sha256: String,
    /// Exactly one for the initial lossless Arrow representation profile.
    pub normalizer_version: u32,
}

impl PinnedSourceVector {
    /// Validate complete source names, fixed metadata and actual Arrow layouts.
    ///
    /// The IO adapter separately validates the real catalog UUID, complete
    /// Iceberg schema semantics, snapshot descriptor and retained ancestry.
    pub fn validate_for(&self, tables: &BTreeMap<String, RecordBatch>) -> Result<()> {
        if self.version != 1 || self.sources.is_empty() || self.sources.len() != tables.len() {
            return Err(invalid(
                "invalid source-vector version or complete source set",
            ));
        }
        for (name, source) in &self.sources {
            if !nonempty(name) {
                return Err(invalid("source-vector names must be nonempty"));
            }
            let batch = tables
                .get(name)
                .ok_or_else(|| invalid("source-vector names do not match checkpoint tables"))?;
            source.validate(batch)?;
        }
        Ok(())
    }

    /// Validate fixed bindings and strictly advancing changed cursors.
    ///
    /// Sequence monotonicity is necessary but does not prove ancestry; the IO
    /// adapter must establish actual parent links before publication.
    pub fn validate_successor(&self, before: &Self) -> Result<()> {
        if self.version != before.version || self.sources.len() != before.sources.len() {
            return Err(invalid("a source vector cannot change its source bindings"));
        }
        let mut changed = false;
        for (name, previous) in &before.sources {
            let next = self
                .sources
                .get(name)
                .ok_or_else(|| invalid("a source vector cannot change its source names"))?;
            let mut binding = next.clone();
            binding.snapshot = previous.snapshot.clone();
            if binding != *previous {
                return Err(invalid(
                    "a source vector cannot change its immutable binding or schema",
                ));
            }
            if next.snapshot == previous.snapshot {
                continue;
            }
            match (&previous.snapshot, &next.snapshot) {
                (PinnedSnapshot::Empty, PinnedSnapshot::Snapshot { .. }) => {}
                (
                    PinnedSnapshot::Snapshot {
                        id: old_id,
                        sequence_number: old_sequence,
                        ..
                    },
                    PinnedSnapshot::Snapshot {
                        id,
                        sequence_number,
                        ..
                    },
                ) if id != old_id && sequence_number > old_sequence => {}
                _ => {
                    return Err(invalid(
                        "source cursor is not a strictly advancing endpoint",
                    ))
                }
            }
            changed = true;
        }
        if !changed {
            return Err(invalid(
                "an identical source vector cannot create another epoch",
            ));
        }
        Ok(())
    }

    /// SHA-256 of the exact canonical `source-vector.json` file bytes.
    ///
    /// The versioned JSON object supplies the format domain. This digest is
    /// deliberately identical to the manifest's file digest.
    pub fn digest(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(self.canonical_bytes()?)))
    }

    pub(crate) fn canonical_bytes(&self) -> Result<Vec<u8>> {
        // Check opaque JSON before serde recursively copies it into the envelope.
        for source in self.sources.values() {
            canonical_bytes(&source.iceberg_schema)?;
            canonical_bytes(&source.arrow_layout)?;
        }
        canonical_bytes(&serde_json::to_value(self)?)
    }
}

impl PinnedSource {
    fn validate(&self, batch: &RecordBatch) -> Result<()> {
        let root = Path::new(&self.catalog.canonical_root);
        let normalized: PathBuf = root.components().collect();
        if self.catalog.kind != "local/1"
            || !root.is_absolute()
            || root
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
            || normalized.to_str() != Some(self.catalog.canonical_root.as_str())
            || !nonempty(&self.catalog.canonical_root)
            || self.namespace.is_empty()
            || self.namespace.iter().any(|part| !nonempty(part))
            || !nonempty(&self.name)
            || !canonical_uuid(&self.table_uuid)
            || self.reference != "main"
            || self.schema_id < 0
            || self.normalizer_version != 1
        {
            return Err(invalid("invalid fixed source binding"));
        }
        match &self.snapshot {
            PinnedSnapshot::Snapshot {
                sequence_number,
                descriptor_sha256,
                ..
            } if *sequence_number <= 0 || !valid_digest(descriptor_sha256) => {
                return Err(invalid("invalid source snapshot cursor"));
            }
            PinnedSnapshot::Empty if batch.num_rows() != 0 => {
                return Err(invalid(
                    "an initial Empty source cursor cannot contain rows",
                ));
            }
            _ => {}
        }
        if self.iceberg_schema_sha256 != iceberg_schema_digest(&self.iceberg_schema)?
            || self.arrow_layout_sha256 != arrow_layout_digest(&self.arrow_layout)?
            || self.arrow_layout != canonical_arrow_layout(batch.schema().as_ref())?
        {
            return Err(invalid(
                "source schema digest or complete Arrow layout mismatch",
            ));
        }
        let fields = self
            .iceberg_schema
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("source Iceberg schema must contain all fields"))?;
        if self.iceberg_schema.get("schema-id").and_then(Value::as_i64)
            != Some(i64::from(self.schema_id))
            || self.iceberg_schema.get("type").and_then(Value::as_str) != Some("struct")
            || fields.len() != batch.num_columns()
        {
            return Err(invalid(
                "source Iceberg schema identity or field count mismatch",
            ));
        }
        let mut identifiers = BTreeSet::new();
        for ((field, column), source_field) in batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(fields)
        {
            let id = source_field
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid("source Iceberg field has no identifier"))?;
            if id <= 0
                || id > i64::from(i32::MAX)
                || !identifiers.insert(id)
                || source_field.get("name").and_then(Value::as_str) != Some(field.name().as_str())
                || source_field.get("required").and_then(Value::as_bool)
                    != Some(!field.is_nullable())
                || !source_field
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|source_type| iceberg_type_matches(source_type, field.data_type()))
                || (!field.is_nullable() && column.null_count() != 0)
            {
                return Err(invalid(
                    "source Iceberg/Arrow field identity or requiredness mismatch",
                ));
            }
        }
        Ok(())
    }
}

fn iceberg_type_matches(source: &str, arrow: &DataType) -> bool {
    match arrow {
        DataType::Boolean => source == "boolean",
        DataType::Int32 => source == "int",
        DataType::Int64 => source == "long",
        DataType::Utf8 => source == "string",
        DataType::Date32 => source == "date",
        DataType::Time64(TimeUnit::Microsecond) => source == "time",
        DataType::Timestamp(TimeUnit::Microsecond, None) => source == "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => source == "timestamptz",
        DataType::Decimal128(precision, scale) => {
            *precision > 0
                && *precision <= 38
                && *scale >= 0
                && *scale as u8 <= *precision
                && source
                    .chars()
                    .filter(|character| !character.is_ascii_whitespace())
                    .collect::<String>()
                    == format!("decimal({precision},{scale})")
        }
        _ => false,
    }
}

/// Describe the complete qualified Arrow schema with recursively ordered metadata.
///
/// Unsupported types fail explicitly; source IO cannot silently cast them into
/// this initial scalar profile.
pub fn canonical_arrow_layout(schema: &Schema) -> Result<Value> {
    let mut names = BTreeSet::new();
    let mut fields = Vec::new();
    for field in schema.fields() {
        if !nonempty(field.name()) || !names.insert(field.name()) {
            return Err(invalid(
                "source Arrow field names must be nonempty and unique",
            ));
        }
        let data_type = match field.data_type() {
            DataType::Boolean => json!({"name":"boolean"}),
            DataType::Int32 => json!({"name":"int32"}),
            DataType::Int64 => json!({"name":"int64"}),
            DataType::Utf8 => json!({"name":"utf8"}),
            DataType::Decimal128(precision, scale) => {
                json!({"name":"decimal128","precision":precision,"scale":scale})
            }
            DataType::Date32 => json!({"name":"date32"}),
            DataType::Time64(TimeUnit::Microsecond) => {
                json!({"name":"time64","unit":"microsecond"})
            }
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                json!({"name":"timestamp","unit":"microsecond","timezone":timezone.as_deref()})
            }
            other => return Err(invalid(format!("unsupported source Arrow type: {other}"))),
        };
        fields.push(json!({
            "name":field.name(), "nullable":field.is_nullable(), "data_type":data_type,
            "metadata":field.metadata().iter().collect::<BTreeMap<_,_>>()
        }));
    }
    Ok(json!({"fields":fields,"metadata":schema.metadata().iter().collect::<BTreeMap<_,_>>()}))
}

/// Domain-separated digest of complete Iceberg schema JSON.
pub fn iceberg_schema_digest(schema: &Value) -> Result<String> {
    metadata_digest(b"optd-incremental-sql/iceberg-schema/1\0", schema)
}

/// Domain-separated digest of the complete canonical Arrow layout JSON.
pub fn arrow_layout_digest(layout: &Value) -> Result<String> {
    metadata_digest(b"optd-incremental-sql/arrow-layout/1\0", layout)
}

/// Domain-separated digest of an immutable Iceberg snapshot descriptor.
pub fn snapshot_descriptor_digest(descriptor: &Value) -> Result<String> {
    metadata_digest(b"optd-incremental-sql/iceberg-snapshot/1\0", descriptor)
}

fn metadata_digest(domain: &[u8], value: &Value) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(canonical_bytes(value)?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn canonical_bytes(value: &Value) -> Result<Vec<u8>> {
    fn append(value: &Value, output: &mut Vec<u8>, depth: usize) -> Result<()> {
        if depth > MAX_METADATA_DEPTH {
            return Err(invalid("source metadata exceeds its nesting limit"));
        }
        match value {
            Value::Object(object) => {
                output.push(b'{');
                let ordered: BTreeMap<_, _> = object.iter().collect();
                for (index, (key, value)) in ordered.into_iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    serde_json::to_writer(&mut *output, key)?;
                    output.push(b':');
                    append(value, output, depth + 1)?;
                }
                output.push(b'}');
            }
            Value::Array(array) => {
                output.push(b'[');
                for (index, item) in array.iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    append(item, output, depth + 1)?;
                }
                output.push(b']');
            }
            _ => serde_json::to_writer(&mut *output, value)?,
        }
        if output.len() > MAX_METADATA_BYTES {
            return Err(invalid("source metadata exceeds its byte limit"));
        }
        Ok(())
    }
    let mut output = Vec::new();
    append(value, &mut output, 0)?;
    Ok(output)
}

pub(crate) fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

fn nonempty(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
