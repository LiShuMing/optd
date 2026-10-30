//! Verify expanded Optd plans before either bootstrap or physical execution.

use super::plan::{
    ApplyMode, IncrementalNode, IncrementalRel, Operation, RelationOperator, StateSpec,
};
use super::{RuleContext, SnapshotVersion};
use crate::{Error, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use optd_core::nodes::PlanNodeOrGroup;
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Type {
    Relation(SchemaRef),
    Projected(SchemaRef, SchemaRef),
    Index(usize),
    Group(usize),
}
fn invalid(message: &str) -> Error {
    Error::Invalid(format!("incremental Optd plan: {message}"))
}
fn relation(typ: &Type) -> Result<SchemaRef> {
    match typ {
        Type::Relation(schema) => Ok(schema.clone()),
        _ => Err(invalid("expected a relation input")),
    }
}
fn arity(types: &[Type], count: usize) -> Result<()> {
    if types.len() == count {
        Ok(())
    } else {
        Err(invalid("operation arity differs"))
    }
}
fn apply(operator: &RelationOperator, types: &[Type]) -> Result<Type> {
    arity(types, operator.inputs.len())?;
    for (input, schema) in types.iter().zip(&operator.inputs) {
        if relation(input)?.as_ref() != schema.as_arrow() {
            return Err(invalid("operator input schema differs"));
        }
    }
    // Operator templates are closed expressions over schema-only ports. No real
    // table provider or original graph input may be reached at execution time.
    fn ports(plan: &LogicalPlan, found: &mut Vec<datafusion::common::DFSchemaRef>) -> Result<()> {
        if let LogicalPlan::EmptyRelation(empty) = plan {
            if empty.produce_one_row {
                return Err(invalid("operator port produces a row"));
            }
            found.push(empty.schema.clone());
            return Ok(());
        }
        if plan.inputs().is_empty() {
            return Err(invalid("operator template has an unbound leaf"));
        }
        for child in plan.inputs() {
            ports(child, found)?;
        }
        Ok(())
    }
    let mut found = vec![];
    ports(&operator.template, &mut found)?;
    if found != operator.inputs {
        return Err(invalid("operator placeholder schemas differ"));
    }
    Ok(Type::Relation(operator.schema()))
}

pub(super) fn verify(
    root: &IncrementalRel,
    physical: bool,
    context: &RuleContext<'_>,
    state: &Option<StateSpec>,
    reads: &std::collections::BTreeMap<usize, SchemaRef>,
) -> Result<()> {
    struct Check<'a, 'b> {
        physical: bool,
        reads: &'a std::collections::BTreeMap<usize, SchemaRef>,
        context: &'a RuleContext<'b>,
        state: &'a Option<StateSpec>,
        count: usize,
        preparations: HashSet<IncrementalRel>,
        completions: usize,
    }
    impl Check<'_, '_> {
        fn node(&mut self, node: &IncrementalRel, depth: usize) -> Result<Type> {
            self.count += 1;
            if self.count > 16384 || depth > 256 {
                return Err(invalid("expanded plan exceeds size/depth budget"));
            }
            if !node.predicates.is_empty() {
                return Err(invalid("unbound Optd predicates"));
            }
            let operation = match (&node.typ, self.physical) {
                (IncrementalNode::Logical(op), false) | (IncrementalNode::Physical(op), true) => op,
                _ => {
                    return Err(invalid(
                        "unexpanded snapshot or incorrect logical/physical phase",
                    ))
                }
            };
            let types = node
                .children
                .iter()
                .map(|child| match child {
                    PlanNodeOrGroup::PlanNode(child) => self.node(child, depth + 1),
                    PlanNodeOrGroup::Group(_) => Err(invalid("unresolved memo group")),
                })
                .collect::<Result<Vec<_>>>()?;
            let output = match operation {
                Operation::UpdateAccumulator { state, schema } => {
                    arity(&types, 1)?;
                    let Some(StateSpec::Accumulator {
                        id,
                        input,
                        operator,
                    }) = self.state
                    else {
                        return Err(invalid("missing accumulator declaration"));
                    };
                    if depth != 0
                        || state != id
                        || !self.context.accumulator_eligible()
                        || schema != &operator.schema()
                        || types[0]
                            != Type::Relation(std::sync::Arc::new(
                                operator.inputs[0].as_arrow().clone(),
                            ))
                    {
                        return Err(invalid("accumulator state/schema differs"));
                    }
                    let PlanNodeOrGroup::PlanNode(child) = &node.children[0] else {
                        return Err(invalid("unbound accumulator delta"));
                    };
                    if !matches!(&child.typ, IncrementalNode::Logical(Operation::DeltaRead {input:i,..}) | IncrementalNode::Physical(Operation::DeltaRead {input:i,..}) if i == input)
                    {
                        return Err(invalid("accumulator must read its declared input delta"));
                    }
                    self.preparations.insert(node.clone());
                    self.completions += 1;
                    Type::Relation(schema.clone())
                }
                Operation::Empty(schema) => {
                    arity(&types, 0)?;
                    Type::Relation(schema.clone())
                }
                Operation::DeltaRead { input, schema }
                | Operation::SnapshotRead { input, schema, .. } => {
                    arity(&types, 0)?;
                    if *input > self.context.node_id()
                        || schema
                            != self
                                .reads
                                .get(input)
                                .ok_or_else(|| invalid("relation read escapes declared bindings"))?
                    {
                        return Err(invalid("relation binding schema differs"));
                    }
                    if *input == self.context.node_id()
                        && !matches!(
                            operation,
                            Operation::SnapshotRead {
                                version: SnapshotVersion::Before,
                                ..
                            }
                        )
                    {
                        return Err(invalid("self read must reference committed output"));
                    }
                    Type::Relation(schema.clone())
                }
                Operation::SourceRead { name, schema, .. } => {
                    arity(&types, 0)?;
                    let LogicalPlan::TableScan(scan) = self.context.logical_plan() else {
                        return Err(invalid("source read at a non-source operator"));
                    };
                    if name != &scan.table_name.to_string() || schema != &scan.source.schema() {
                        return Err(invalid("source binding differs"));
                    }
                    Type::Relation(schema.clone())
                }
                Operation::Sum => {
                    let first = types.first().ok_or_else(|| invalid("empty sum"))?;
                    relation(first)?;
                    if types.iter().any(|t| t != first) {
                        return Err(invalid("sum types differ"));
                    }
                    first.clone()
                }
                Operation::Negate => {
                    arity(&types, 1)?;
                    relation(&types[0])?;
                    types[0].clone()
                }
                Operation::Apply { operator, mode } => {
                    match mode {
                        ApplyMode::SignedJoin if !matches!(operator.template.as_ref(), LogicalPlan::Join(j) if j.join_type == JoinType::Inner) => {
                            return Err(invalid("signed join requires inner join"))
                        }
                        ApplyMode::SignedLinear
                            if !matches!(
                                operator.template.as_ref(),
                                LogicalPlan::Projection(_)
                                    | LogicalPlan::Filter(_)
                                    | LogicalPlan::SubqueryAlias(_)
                                    | LogicalPlan::Repartition(_)
                                    | LogicalPlan::Union(_)
                                    | LogicalPlan::EmptyRelation(_)
                            ) =>
                        {
                            return Err(invalid("operator is not jointly linear"))
                        }
                        _ => {}
                    }
                    apply(operator, &types)?
                }
                Operation::ProjectKeys(projection) => {
                    arity(&types, 1)?;
                    let Type::Relation(keys) = apply(projection, &types)? else {
                        return Err(invalid("invalid projection type"));
                    };
                    Type::Projected(relation(&types[0])?, keys)
                }
                Operation::PrepareIndex(spec) => {
                    if self.state.as_ref() != Some(&StateSpec::Keyed(spec.clone())) {
                        return Err(invalid("index declaration differs"));
                    }
                    arity(&types, spec.projections.len())?;
                    for (typ, projection) in types.iter().zip(&spec.projections) {
                        if *typ
                            != Type::Projected(
                                std::sync::Arc::new(projection.inputs[0].as_arrow().clone()),
                                projection.schema(),
                            )
                        {
                            return Err(invalid("index projection types differ"));
                        }
                    }
                    self.preparations.insert(node.clone());
                    Type::Index(spec.id)
                }
                Operation::IndexLookup {
                    state,
                    port,
                    version,
                    schema,
                } => {
                    arity(&types, 1)?;
                    if types[0] != Type::Index(*state) {
                        return Err(invalid("index lookup token differs"));
                    }
                    let Some(StateSpec::Keyed(spec)) = self.state else {
                        return Err(invalid("missing index declaration"));
                    };
                    if spec.scope == super::KeyScope::WindowPartitions
                        && *version == SnapshotVersion::Before
                    {
                        return Err(invalid(
                            "window Before input lookup is not materialized; use cached output",
                        ));
                    }
                    let projection = spec
                        .projections
                        .get(*port)
                        .ok_or_else(|| invalid("invalid index port"))?;
                    if schema.as_ref() != projection.inputs[0].as_arrow()
                        || (*version == SnapshotVersion::Before && !spec.before)
                        || (*version == SnapshotVersion::After && !spec.after)
                    {
                        return Err(invalid("index lookup schema/version differs"));
                    }
                    Type::Relation(schema.clone())
                }
                Operation::CachedOutput { state, schema } => {
                    arity(&types, 1)?;
                    if types[0] != Type::Index(*state) {
                        return Err(invalid("cached output token differs"));
                    }
                    let Some(StateSpec::Keyed(spec)) = self.state else {
                        return Err(invalid("missing cached output declaration"));
                    };
                    if !spec.before
                        || spec.output_projection.is_none()
                        || schema.as_ref() != self.context.logical_plan().schema().as_arrow()
                    {
                        return Err(invalid("invalid cached output read"));
                    }
                    Type::Relation(schema.clone())
                }
                Operation::EvaluateSnapshot {
                    state,
                    version,
                    operator,
                } => {
                    let Some(StateSpec::Keyed(spec)) = self.state else {
                        return Err(invalid("missing snapshot state"));
                    };
                    if spec.scope == super::KeyScope::WindowPartitions
                        && *version == SnapshotVersion::Before
                    {
                        return Err(invalid("window Before must read committed cached output"));
                    }
                    if state != &spec.id
                        || (*version == SnapshotVersion::Before && !spec.before)
                        || (*version == SnapshotVersion::After && !spec.after)
                    {
                        return Err(invalid("snapshot state/version differs"));
                    }
                    for (port, child) in node.children.iter().enumerate() {
                        let PlanNodeOrGroup::PlanNode(child) = child else {
                            return Err(invalid("unresolved lookup"));
                        };
                        let op = match &child.typ {
                            IncrementalNode::Logical(op) | IncrementalNode::Physical(op) => op,
                            _ => return Err(invalid("unexpanded lookup")),
                        };
                        if !matches!(op, Operation::IndexLookup { state: s, port: p, version: v, .. } if s == state && *p == port && v == version)
                        {
                            return Err(invalid(
                                "snapshot must read corresponding index ports/version",
                            ));
                        }
                    }
                    apply(operator, &types)?
                }
                Operation::CompleteIndex(spec) => {
                    if depth != 0 {
                        return Err(invalid("index completion must be the program root"));
                    }
                    arity(&types, 2)?;
                    if self.state.as_ref() != Some(&StateSpec::Keyed(spec.clone()))
                        || types[0] != Type::Index(spec.id)
                    {
                        return Err(invalid("index completion differs"));
                    }
                    self.completions += 1;
                    relation(&types[1])?;
                    types[1].clone()
                }
                Operation::PrepareGroups { state } => {
                    arity(&types, 1)?;
                    let Some(StateSpec::Group { id, projection, .. }) = self.state else {
                        return Err(invalid("missing group state"));
                    };
                    if state != id
                        || types[0]
                            != Type::Projected(
                                std::sync::Arc::new(projection.inputs[0].as_arrow().clone()),
                                projection.schema(),
                            )
                    {
                        return Err(invalid("group preparation differs"));
                    }
                    self.preparations.insert(node.clone());
                    Type::Group(*state)
                }
                Operation::GroupLookup { state, schema } => {
                    arity(&types, 1)?;
                    let Some(StateSpec::Group { id, projection, .. }) = self.state else {
                        return Err(invalid("missing group declaration"));
                    };
                    if state != id
                        || types[0] != Type::Group(*state)
                        || schema.as_ref() != projection.inputs[0].as_arrow()
                    {
                        return Err(invalid("group lookup differs"));
                    }
                    Type::Relation(schema.clone())
                }
                Operation::CompleteGroups { state } => {
                    if depth != 0 {
                        return Err(invalid("group completion must be the program root"));
                    }
                    arity(&types, 2)?;
                    if types[0] != Type::Group(*state) {
                        return Err(invalid("group completion token differs"));
                    }
                    self.completions += 1;
                    relation(&types[1])?;
                    types[1].clone()
                }
                Operation::Replace => {
                    if depth != 0 {
                        return Err(invalid("replacement must be the program root"));
                    }
                    arity(&types, 2)?;
                    relation(&types[0])?;
                    if types[0] != types[1] {
                        return Err(invalid("replacement schemas differ"));
                    }
                    types[0].clone()
                }
            };
            Ok(output)
        }
    }
    let mut check = Check {
        physical,
        reads,
        context,
        state,
        count: 0,
        preparations: HashSet::new(),
        completions: 0,
    };
    let result = check.node(root, 0)?;
    if relation(&result)?.as_ref() != context.logical_plan().schema().as_arrow() {
        return Err(invalid("root output schema differs"));
    }
    let expected = usize::from(state.is_some());
    if check.preparations.len() != expected || check.completions != expected {
        return Err(invalid(
            "state must have one shared preparation and one completion",
        ));
    }
    if context.ordered()
        && !matches!(
            &root.typ,
            IncrementalNode::Logical(Operation::Replace)
                | IncrementalNode::Physical(Operation::Replace)
        )
    {
        return Err(invalid(
            "ordered output must retain its replacement sequence",
        ));
    }
    Ok(())
}
