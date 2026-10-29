// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! The `optd-cdc/1` wire contract. See `iceberg_cdc_output.md`.
//! Files and timestamps are deliberately excluded from batch identity.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};

use crate::change::{Multiset, Row, TableId, Value};
use crate::plan::{children_of, AggKind, Expr, IncNodeType, IncPlanNode};

/// Version of the canonical change table and its summary envelope.
pub const PROTOCOL: &str = "optd-cdc/1";

/// A failed protocol validation, ownership check or storage operation.
#[derive(Debug)]
pub enum CdcError {
    /// Schema is ambiguous or unsupported.
    InvalidSchema,
    /// A payload or metadata row violates the contract.
    InvalidRow,
    /// Source identity, sequence or definition does not match.
    InvalidCheckpoint,
    /// Envelope or canonical value is malformed.
    InvalidEnvelope,
    /// Canonical digest does not match the declared value.
    DigestMismatch,
    /// The declared profile is unsupported by this operation.
    UnsupportedProfile,
    /// A bounded protocol integer overflowed.
    Overflow,
    /// Another process holds the required lock.
    Ownership,
    /// A different output successor has appeared.
    Conflict,
    /// The previous attempt's outcome cannot yet be determined.
    UnknownCommit,
    /// A source or view evaluation failed.
    Evaluation(crate::engine::RefreshError),
    /// Local IO failed.
    Io(std::io::Error),
    /// Serialization failed.
    Json(serde_json::Error),
    /// An underlying storage adapter failed.
    Storage(String),
}
impl fmt::Display for CdcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evaluation(e) => write!(f, "CDC evaluation: {e}"),
            Self::Io(e) => write!(f, "CDC IO: {e}"),
            Self::Json(e) => write!(f, "CDC JSON: {e}"),
            Self::Storage(e) => write!(f, "CDC storage: {e}"),
            other => write!(f, "CDC: {other:?}"),
        }
    }
}
impl std::error::Error for CdcError {}
impl From<std::io::Error> for CdcError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for CdcError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
impl From<crate::engine::RefreshError> for CdcError {
    fn from(e: crate::engine::RefreshError) -> Self {
        Self::Evaluation(e)
    }
}

/// Encode the JSON-compatible subset of C1. Floats and unsigned values above i64 fail.
/// Binary tag 4 has no use in the v1 JSON envelope or Int/Str payload descriptors.
pub fn canonical_encode(value: &Json) -> Result<Vec<u8>, CdcError> {
    fn put(v: &Json, out: &mut Vec<u8>, depth: usize) -> Result<(), CdcError> {
        if depth > 128 {
            return Err(CdcError::InvalidEnvelope);
        }
        match v {
            Json::Null => out.push(0),
            Json::Bool(b) => {
                out.extend([1, u8::from(*b)]);
            }
            Json::Number(n) => {
                out.push(2);
                out.extend(n.as_i64().ok_or(CdcError::InvalidEnvelope)?.to_be_bytes());
            }
            Json::String(s) => {
                out.push(3);
                out.extend(
                    u64::try_from(s.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                out.extend(s.as_bytes());
            }
            Json::Array(a) => {
                out.push(5);
                out.extend(
                    u64::try_from(a.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                for item in a {
                    put(item, out, depth + 1)?;
                }
            }
            Json::Object(m) => {
                out.push(6);
                out.extend(
                    u64::try_from(m.len())
                        .map_err(|_| CdcError::Overflow)?
                        .to_be_bytes(),
                );
                let sorted: BTreeMap<_, _> = m.iter().collect();
                for (key, item) in sorted {
                    put(&json!(key), out, depth + 1)?;
                    put(item, out, depth + 1)?;
                }
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    put(value, &mut out, 0)?;
    Ok(out)
}

/// Stable SHA-256 fingerprint of a domain-separated C1 descriptor.
pub fn fingerprint(descriptor: &Json) -> Result<String, CdcError> {
    let digest = Sha256::digest(canonical_encode(descriptor)?);
    Ok(format!("sha256:{digest:x}"))
}

/// Parse without accepting duplicate object keys or lossy numeric conversions.
pub fn parse_unique_json(input: &str) -> Result<Json, CdcError> {
    struct Unique(Json);
    impl<'de> Deserialize<'de> for Unique {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Unique;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "lossless JSON with unique keys")
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<Unique, E> {
                    Ok(Unique(Json::Null))
                }
                fn visit_bool<E: serde::de::Error>(self, b: bool) -> Result<Unique, E> {
                    Ok(Unique(json!(b)))
                }
                fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Unique, E> {
                    Ok(Unique(json!(n)))
                }
                fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Unique, E> {
                    Ok(Unique(json!(i64::try_from(n).map_err(E::custom)?)))
                }
                fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Unique, E> {
                    Ok(Unique(json!(s)))
                }
                fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Unique, E> {
                    Ok(Unique(json!(s)))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut a: A,
                ) -> Result<Unique, A::Error> {
                    let mut values = Vec::new();
                    while let Some(Unique(v)) = a.next_element()? {
                        values.push(v);
                    }
                    Ok(Unique(Json::Array(values)))
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut a: A,
                ) -> Result<Unique, A::Error> {
                    let mut values = serde_json::Map::new();
                    while let Some((k, Unique(v))) = a.next_entry::<String, Unique>()? {
                        if values.insert(k, v).is_some() {
                            return Err(serde::de::Error::custom("duplicate JSON key"));
                        }
                    }
                    Ok(Unique(Json::Object(values)))
                }
            }
            d.deserialize_any(Visitor)
        }
    }
    Ok(serde_json::from_str::<Unique>(input)?.0)
}

fn uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
            }
        })
}
fn hash(s: &str) -> bool {
    s.len() == 71
        && s.starts_with("sha256:")
        && s.as_bytes()[7..]
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
}
fn payload(row: &Row) -> Json {
    Json::Array(
        row.iter()
            .map(|v| match v {
                Value::Null => Json::Null,
                Value::Int(n) => json!(n),
                Value::Str(s) => json!(s),
            })
            .collect(),
    )
}

/// Native payload types supported by the reference evaluator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadType {
    /// A signed integer.
    Long,
    /// A UTF-8 string.
    String,
}
/// A declared view field; its Iceberg id is 1000 plus its position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputField {
    /// Payload name, outside the reserved system prefix.
    pub name: String,
    /// Native representation of the value.
    pub data_type: PayloadType,
    /// Whether NULL is forbidden.
    pub required: bool,
}
/// Declared complete payload schema, including fields currently containing only NULL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputSchema {
    fields: Vec<OutputField>,
    key: Option<KeySpec>,
}
/// Ordered, required payload field ids defining unique generation-scoped row identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeySpec(pub Vec<i32>);
impl OutputSchema {
    /// Validate names and ids before any write is possible.
    pub fn new(fields: Vec<OutputField>) -> Result<Self, CdcError> {
        let mut names = BTreeSet::new();
        for (i, f) in fields.iter().enumerate() {
            i32::try_from(i)
                .ok()
                .and_then(|n| n.checked_add(1000))
                .ok_or(CdcError::Overflow)?;
            if f.name.is_empty()
                || f.name.to_ascii_lowercase().starts_with("__meta_")
                || !names.insert(f.name.to_ascii_lowercase())
            {
                return Err(CdcError::InvalidSchema);
            }
        }
        if fields.is_empty() {
            return Err(CdcError::InvalidSchema);
        }
        Ok(Self { fields, key: None })
    }
    /// Enable keyed annotations; endpoint relations must have unit, unique keys.
    pub fn keyed(mut self, key: KeySpec) -> Result<Self, CdcError> {
        let mut seen = BTreeSet::new();
        if key.0.is_empty() {
            return Err(CdcError::InvalidSchema);
        }
        for id in &key.0 {
            let index = id
                .checked_sub(1000)
                .and_then(|i| usize::try_from(i).ok())
                .ok_or(CdcError::InvalidSchema)?;
            if !seen.insert(*id) || !self.fields.get(index).is_some_and(|f| f.required) {
                return Err(CdcError::InvalidSchema);
            }
        }
        self.key = Some(key);
        Ok(self)
    }
    /// Immutable key definition, if this generation provides keyed annotations.
    pub fn key(&self) -> Option<&KeySpec> {
        self.key.as_ref()
    }
    /// Wire profile bound by the schema fingerprint.
    pub fn profile(&self) -> &'static str {
        if self.key.is_some() {
            "keyed"
        } else {
            "unkeyed"
        }
    }
    fn key_values(&self, row: &Row) -> Result<Row, CdcError> {
        self.validate_row(row)?;
        self.key
            .as_ref()
            .ok_or(CdcError::UnsupportedProfile)?
            .0
            .iter()
            .map(|id| {
                row.get((*id - 1000) as usize)
                    .filter(|v| **v != Value::Null)
                    .cloned()
                    .ok_or(CdcError::InvalidRow)
            })
            .collect()
    }
    /// Validate the complete endpoint, including unchanged keys and multiplicities.
    pub fn validate_relation(&self, relation: &Multiset) -> Result<(), CdcError> {
        let mut seen = BTreeSet::new();
        for (row, weight) in relation.iter() {
            self.validate_row(row)?;
            if *weight <= 0
                || (self.key.is_some() && (*weight != 1 || !seen.insert(self.key_values(row)?)))
            {
                return Err(CdcError::InvalidRow);
            }
        }
        Ok(())
    }
    /// Payload fields in stable view order.
    pub fn fields(&self) -> &[OutputField] {
        &self.fields
    }
    /// Reject incomplete, untyped or incompatible row images.
    pub fn validate_row(&self, row: &Row) -> Result<(), CdcError> {
        if row.len() != self.fields.len() {
            return Err(CdcError::InvalidRow);
        }
        for (v, f) in row.iter().zip(&self.fields) {
            if !match (v, &f.data_type) {
                (Value::Null, _) => !f.required,
                (Value::Int(_), PayloadType::Long) | (Value::Str(_), PayloadType::String) => true,
                _ => false,
            } {
                return Err(CdcError::InvalidRow);
            }
        }
        Ok(())
    }
    /// Full canonical schema descriptor, including system fields.
    pub fn descriptor(&self) -> Json {
        let mut fields: Vec<Json> = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                json!({
                    "id": 1000 + i, "name": f.name, "type": f.data_type, "required": f.required
                })
            })
            .collect();
        for (id, name, typ, required) in system_fields() {
            fields.push(json!({ "id": id, "name": name, "type": typ, "required": required }));
        }
        json!([
            "optd-schema/1",
            "value-v1",
            self.profile(),
            self.key.as_ref().map(|k| k.0.clone()).unwrap_or_default(),
            fields
        ])
    }
}

/// Fixed metadata field ids, names, Iceberg type names and requiredness.
pub fn system_fields() -> [(i32, &'static str, &'static str, bool); 8] {
    [
        (1, "__meta_op_type", "int", true),
        (2, "__meta_is_update", "boolean", true),
        (3, "__meta_weight", "long", true),
        (4, "__meta_row_id", "string", false),
        (5, "__meta_batch_id", "string", true),
        (6, "__meta_batch_seq", "long", true),
        (7, "__meta_ordinal", "long", true),
        (8, "__meta_timestamp", "timestamptz", true),
    ]
}

/// Stable source identity bound to a logical scan in the view definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBinding {
    /// Iceberg table UUID, not a source-local TableId.
    pub table_id: String,
    /// Branch name, currently main.
    pub reference: String,
}
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
        if !uuid(&p.table_id)
            || p.reference != "main"
            || !hash(&p.schema_fingerprint)
            || p.snapshot_id.is_some() != p.sequence_number.is_some()
            || p.sequence_number.is_some_and(|s| s < 0)
        {
            return Err(CdcError::InvalidCheckpoint);
        }
    }
    Ok(())
}

fn expression(expr: &Expr) -> Json {
    let (kind, params, children) = match expr {
        Expr::Col(c) => ("Col", json!({"column": c}), vec![]),
        Expr::Lit(v) => (
            "Lit",
            json!({"value": payload(&vec![v.clone()])[0]}),
            vec![],
        ),
        Expr::Gt(a, b) => ("Gt", json!({}), vec![expression(a), expression(b)]),
        Expr::Eq(a, b) => ("Eq", json!({}), vec![expression(a), expression(b)]),
    };
    json!({"kind":kind,"params":params,"children":children})
}
fn plan_node(
    plan: &IncPlanNode,
    bindings: &BTreeMap<TableId, SourceBinding>,
) -> Result<Json, CdcError> {
    let (kind, params) = match &plan.typ {
        IncNodeType::Scan(t) => {
            let binding = bindings.get(t).ok_or(CdcError::InvalidCheckpoint)?;
            ("Scan", serde_json::to_value(binding)?)
        }
        IncNodeType::EmptyRelation => ("EmptyRelation", json!({})),
        IncNodeType::Projection(cols) => ("Projection", json!({"columns":cols})),
        IncNodeType::Filter => ("Filter", json!({})),
        IncNodeType::Join(keys) => ("Join", json!({"keys":keys})),
        IncNodeType::Union => ("Union", json!({})),
        IncNodeType::Agg { groups, aggs } => {
            let aggs: Vec<Json> = aggs
                .iter()
                .map(|a| match a {
                    AggKind::Count => json!({"kind":"Count"}),
                    AggKind::Sum(c) => json!({"kind":"Sum","column":c}),
                })
                .collect();
            ("Agg", json!({"groups":groups,"aggs":aggs}))
        }
        // Derived/internal delta operators are never a persisted view definition.
        _ => return Err(CdcError::InvalidSchema),
    };
    let children = children_of(plan);
    if children.len() != plan.children.len() {
        return Err(CdcError::InvalidSchema);
    }
    let children: Vec<Json> = children
        .into_iter()
        .map(|p| plan_node(p, bindings))
        .collect::<Result<_, _>>()?;
    let predicates: Vec<Json> = plan.predicates.iter().map(|p| expression(&p.typ)).collect();
    Ok(json!({"kind":kind,"params":params,"children":children,"predicates":predicates}))
}

/// Immutable output-generation definition. Descriptors are verified on every catalog open.
#[derive(Clone, Debug)]
pub struct Generation {
    view_id: String,
    generation_id: String,
    schema: OutputSchema,
    plan_descriptor: Json,
    bindings: BTreeMap<TableId, SourceBinding>,
}
impl Generation {
    /// Bind a concrete view to durable source UUIDs and a declared result schema.
    pub fn new(
        view_id: String,
        generation_id: String,
        schema: OutputSchema,
        plan: &IncPlanNode,
        bindings: BTreeMap<TableId, SourceBinding>,
    ) -> Result<Self, CdcError> {
        if !uuid(&view_id) || !uuid(&generation_id) || bindings.len() != 1 {
            return Err(CdcError::InvalidCheckpoint);
        }
        for b in bindings.values() {
            if !uuid(&b.table_id) || b.reference != "main" {
                return Err(CdcError::InvalidCheckpoint);
            }
        }
        if crate::engine::distinct_tables(plan) != bindings.keys().copied().collect::<Vec<_>>() {
            return Err(CdcError::InvalidCheckpoint);
        }
        let plan_descriptor = json!(["optd-plan/1", plan_node(plan, &bindings)?]);
        Ok(Self {
            view_id,
            generation_id,
            schema,
            plan_descriptor,
            bindings,
        })
    }
    /// View UUID.
    pub fn view_id(&self) -> &str {
        &self.view_id
    }
    /// Generation UUID.
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }
    /// Declared output schema.
    pub fn schema(&self) -> &OutputSchema {
        &self.schema
    }
    /// Durable source binding map.
    pub fn bindings(&self) -> &BTreeMap<TableId, SourceBinding> {
        &self.bindings
    }
    /// Reversible C1/base64url identity; no full-row hash is substituted for a key.
    pub fn row_id(&self, row: &Row) -> Result<String, CdcError> {
        let key = self.schema.key_values(row)?;
        let ids = &self
            .schema
            .key
            .as_ref()
            .ok_or(CdcError::UnsupportedProfile)?
            .0;
        let values = payload(&key);
        let entries: Vec<Json> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| json!([id, values[i]]))
            .collect();
        Ok(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(canonical_encode(&json!([
                "optd-row/1",
                self.view_id,
                self.generation_id,
                entries
            ]))?),
        )
    }
    /// Immutable catalog properties expected on the output table.
    pub fn properties(&self) -> Result<BTreeMap<String, String>, CdcError> {
        Ok(BTreeMap::from([
            ("optd.protocol".into(), PROTOCOL.into()),
            ("optd.view_id".into(), self.view_id.clone()),
            ("optd.generation_id".into(), self.generation_id.clone()),
            (
                "optd.plan_fingerprint".into(),
                fingerprint(&self.plan_descriptor)?,
            ),
            (
                "optd.schema_fingerprint".into(),
                fingerprint(&self.schema.descriptor())?,
            ),
            (
                "optd.plan_descriptor".into(),
                serde_json::to_string(&self.plan_descriptor)?,
            ),
            (
                "optd.schema_descriptor".into(),
                serde_json::to_string(&self.schema.descriptor())?,
            ),
            ("commit.retry.num-retries".into(), "0".into()),
        ]))
    }
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
    fn identity(&self) -> Result<String, CdcError> {
        fingerprint(&json!([
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
        let envelope: Self = serde_json::from_value(parse_unique_json(input)?)?;
        envelope.validate()?;
        Ok(envelope)
    }
    /// Check structure and deterministic identity before trusting a checkpoint.
    pub fn validate(&self) -> Result<(), CdcError> {
        if !uuid(&self.view_id)
            || !uuid(&self.generation_id)
            || self.batch_seq < 1
            || self.record_count < 0
            || !hash(&self.plan_fingerprint)
            || !hash(&self.schema_fingerprint)
            || !hash(&self.delta_hash)
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
            (Some(from), Some(prev), "delta", seq) if seq > 1 && hash(prev) => {
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
        if self.view_id != generation.view_id
            || self.generation_id != generation.generation_id
            || self.plan_fingerprint != props["optd.plan_fingerprint"]
            || self.schema_fingerprint != props["optd.schema_fingerprint"]
            || self.profile != generation.schema.profile()
        {
            return Err(CdcError::InvalidCheckpoint);
        }
        let binding = generation
            .bindings
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
    fingerprint(&json!([
        "optd-delta/1",
        rows.iter()
            .map(|r| json!([
                payload(&r.payload),
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
    generation.schema.validate_relation(before)?;
    generation.schema.validate_relation(after)?;
    if generation.schema.key.is_none() {
        return records(
            &after
                .try_difference(before)
                .map_err(|_| CdcError::Overflow)?,
            &generation.schema,
        );
    }
    let keyed = |relation: &Multiset| -> Result<BTreeMap<Row, Row>, CdcError> {
        relation
            .iter()
            .map(|(row, _)| Ok((generation.schema.key_values(row)?, row.clone())))
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
        if generation.schema.key.is_some() {
            return Err(CdcError::UnsupportedProfile);
        }
        Self::build(
            generation,
            previous,
            source_to,
            records(delta, &generation.schema)?,
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
            view_id: generation.view_id.clone(),
            generation_id: generation.generation_id.clone(),
            batch_seq: previous.map_or(Ok(1), |p| {
                p.batch_seq.checked_add(1).ok_or(CdcError::Overflow)
            })?,
            previous_batch_id: previous.map(|p| p.batch_id.clone()),
            batch_id: String::new(),
            plan_fingerprint: props["optd.plan_fingerprint"].clone(),
            schema_fingerprint: props["optd.schema_fingerprint"].clone(),
            profile: generation.schema.profile().into(),
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
                || (generation.schema.key.is_none() && (r.is_update || r.row_id.is_some()))
                || (generation.schema.key.is_some()
                    && (r.weight.abs() != 1
                        || r.row_id.as_ref() != Some(&generation.row_id(&r.payload)?)))
                || (envelope.kind == "bootstrap" && r.weight < 0)
            {
                return Err(CdcError::InvalidRow);
            }
            generation.schema.validate_row(&r.payload)?;
            delta
                .try_add_row(r.payload.clone(), r.weight)
                .map_err(|_| CdcError::Overflow)?;
        }
        let expected = if generation.schema.key.is_some() {
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
            records(&delta, &generation.schema)?
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

/// Delta-sized validation against a trusted predecessor relation.
///
/// The caller asserts that `before` is the complete relation at the batch's predecessor
/// checkpoint. This type checks exactly what [`PreparedBatch::apply_to`] checks without
/// materialising the accumulated relation on every batch:
///
/// * an unkeyed batch needs one multiset lookup per record (`O(delta log n)`);
/// * a keyed batch builds one key index over `before` (`O(n log n)`) and then tracks only the
///   keys the batches touch. A producer-side key index would remove that term; until the
///   producer maintains one, keyed validation stays linear in the relation per call.
///
/// Unkeyed rows the delta does not touch are never inspected, and keyed rows with untouched
/// keys are never compared: this is the same blind spot `apply_to` has, not a weaker check.
pub struct TrustedRelation<'a> {
    generation: &'a Generation,
    unkeyed: Option<UnkeyedTrust<'a>>,
    keyed: Option<KeyedTrust>,
}
struct UnkeyedTrust<'a> {
    before: &'a Multiset,
    overlay: Multiset,
}
struct KeyedTrust {
    keys: BTreeMap<Row, Row>,
    /// `None` means the batches deleted the key; `Some` is the row they left behind.
    overlay: BTreeMap<Row, Option<Row>>,
}
impl<'a> TrustedRelation<'a> {
    /// Bind to a trusted relation and check its shape for the generation's profile.
    pub fn new(generation: &'a Generation, before: &'a Multiset) -> Result<Self, CdcError> {
        if generation.schema.key.is_some() {
            let mut keys = BTreeMap::new();
            for (row, weight) in before.iter() {
                if *weight != 1 {
                    return Err(CdcError::InvalidRow);
                }
                let key = generation.schema.key_values(row)?;
                if keys.insert(key, row.clone()).is_some() {
                    return Err(CdcError::InvalidRow);
                }
            }
            Ok(Self {
                generation,
                unkeyed: None,
                keyed: Some(KeyedTrust {
                    keys,
                    overlay: BTreeMap::new(),
                }),
            })
        } else {
            Ok(Self {
                generation,
                unkeyed: Some(UnkeyedTrust {
                    before,
                    overlay: Multiset::new(),
                }),
                keyed: None,
            })
        }
    }
    /// Validate one batch against the effective predecessor relation, then advance it.
    pub fn apply(&mut self, batch: &PreparedBatch) -> Result<(), CdcError> {
        batch.envelope.validate_generation(self.generation)?;
        if let Some(keyed) = &mut self.keyed {
            for record in &batch.rows {
                let key = self.generation.schema.key_values(&record.payload)?;
                let current = match keyed.overlay.get(&key) {
                    Some(entry) => entry.as_ref(),
                    None => keyed.keys.get(&key),
                };
                if record.weight < 0 {
                    if current != Some(&record.payload) {
                        return Err(CdcError::InvalidRow);
                    }
                    keyed.overlay.insert(key, None);
                } else {
                    if current.is_some() {
                        return Err(CdcError::InvalidRow);
                    }
                    keyed.overlay.insert(key, Some(record.payload.clone()));
                }
            }
        } else if let Some(unkeyed) = &mut self.unkeyed {
            for record in &batch.rows {
                let prior = unkeyed
                    .before
                    .weight_of(&record.payload)
                    .checked_add(unkeyed.overlay.weight_of(&record.payload))
                    .ok_or(CdcError::Overflow)?;
                let next = prior.checked_add(record.weight).ok_or(CdcError::Overflow)?;
                if next < 0 {
                    return Err(CdcError::InvalidRow);
                }
                unkeyed
                    .overlay
                    .try_add_row(record.payload.clone(), record.weight)
                    .map_err(|_| CdcError::Overflow)?;
            }
        } else {
            return Err(CdcError::InvalidSchema);
        }
        Ok(())
    }
}

/// Output checkpoint authority; implementations return committed metadata, never just `()`.
pub trait DeltaSink {
    /// Inspect the durable producer checkpoint without advancing it.
    fn last_checkpoint(
        &self,
    ) -> impl Future<Output = Result<Option<CommitEnvelope>, CdcError>> + Send;
    /// Publish the exact prepared batch, resolving duplicates before another append.
    fn emit(
        &mut self,
        batch: &PreparedBatch,
    ) -> impl Future<Output = Result<CommitEnvelope, CdcError>> + Send;
}
