// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Stable view and source identities bound to one output schema.

use super::canonical;
use super::commit_envelope::PROTOCOL;
use super::error::CdcError;
use super::output_schema::OutputSchema;
use crate::change::{Row, TableId};
use crate::plan::{children_of, AggKind, Expr, IncNodeType, IncPlanNode};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;

/// Stable source identity bound to a logical scan in the view definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBinding {
    /// Iceberg table UUID, not a source-local TableId.
    pub table_id: String,
    /// Branch name, currently main.
    pub reference: String,
}
fn expression(expr: &Expr) -> Json {
    let (kind, params, children) = match expr {
        Expr::Col(c) => ("Col", json!({"column": c}), vec![]),
        Expr::Lit(v) => (
            "Lit",
            json!({"value": canonical::payload(&vec![v.clone()])[0]}),
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
        if !canonical::uuid(&view_id) || !canonical::uuid(&generation_id) || bindings.len() != 1 {
            return Err(CdcError::InvalidCheckpoint);
        }
        for b in bindings.values() {
            if !canonical::uuid(&b.table_id) || b.reference != "main" {
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
        let ids = &self.schema.key().ok_or(CdcError::UnsupportedProfile)?.0;
        let values = canonical::payload(&key);
        let entries: Vec<Json> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| json!([id, values[i]]))
            .collect();
        Ok(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(canonical::canonical_encode(
                &json!(["optd-row/1", self.view_id, self.generation_id, entries]),
            )?),
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
                canonical::fingerprint(&self.plan_descriptor)?,
            ),
            (
                "optd.schema_fingerprint".into(),
                canonical::fingerprint(&self.schema.descriptor())?,
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
