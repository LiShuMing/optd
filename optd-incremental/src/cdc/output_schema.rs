// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Declared payload fields, keys and reserved system-field identities.

use super::error::CdcError;
use crate::change::{Multiset, Row, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use std::collections::BTreeSet;

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
    pub(super) fn key_values(&self, row: &Row) -> Result<Row, CdcError> {
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
