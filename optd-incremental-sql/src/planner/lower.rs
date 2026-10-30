//! Compatibility-rule frontend lowering; runtime never reads these expressions.

use super::optd_rules::{matcher, optimize, ExpandScoped, Implement};
use super::plan::{
    logical, lower_steps, ApplyMode, CompiledProgram, IncrementalNode, IncrementalRel, IndexSpec,
    Operation, PlanRule, RelationOperator, StateSpec,
};
use super::{DeltaExpr, KeyScope, Maintenance, RuleContext, SnapshotVersion};
use crate::{Error, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan;
use optd_core::heuristics::ApplyOrder;
use optd_core::nodes::{PlanNode, PlanNodeOrGroup};
use std::sync::Arc;

fn read(input: usize, schema: SchemaRef, version: Option<SnapshotVersion>) -> IncrementalRel {
    logical(
        match version {
            Some(version) => Operation::SnapshotRead {
                input,
                schema,
                version,
            },
            None => Operation::DeltaRead { input, schema },
        },
        vec![],
    )
}

struct Compiler<'a> {
    context: &'a RuleContext<'a>,
    operator: RelationOperator,
    token: Option<IncrementalRel>,
}
impl Compiler<'_> {
    fn input(&self, port: usize, version: Option<SnapshotVersion>) -> IncrementalRel {
        read(
            self.context.inputs()[port],
            Arc::new(
                self.context.logical_plan().inputs()[port]
                    .schema()
                    .as_arrow()
                    .clone(),
            ),
            version,
        )
    }
    fn expression(&self, expr: &DeltaExpr, expected: SchemaRef) -> Result<IncrementalRel> {
        Ok(match expr {
            DeltaExpr::ScopedSnapshot { scope, version } => Arc::new(PlanNode {
                typ: IncrementalNode::ScopedSnapshot {
                    operator: self.operator.clone(),
                    scope: *scope,
                    version: *version,
                    state: self.context.node_id(),
                },
                children: vec![self
                    .token
                    .clone()
                    .ok_or_else(|| Error::Invalid("missing compiled index preparation".into()))?
                    .into()],
                predicates: vec![],
            }),
            DeltaExpr::InputDelta(port) => self.input(*port, None),
            DeltaExpr::InputSnapshot { input, version } => self.input(*input, Some(*version)),
            DeltaExpr::SourceDelta => {
                let LogicalPlan::TableScan(scan) = self.context.logical_plan() else {
                    return Err(Error::Invalid("source delta without source".into()));
                };
                logical(
                    Operation::SourceRead {
                        name: scan.table_name.to_string(),
                        delta: true,
                        schema: scan.source.schema(),
                    },
                    vec![],
                )
            }
            DeltaExpr::Empty => logical(Operation::Empty(expected), vec![]),
            DeltaExpr::Sum(terms) => logical(
                Operation::Sum,
                terms
                    .iter()
                    .map(|e| self.expression(e, expected.clone()))
                    .collect::<Result<_>>()?,
            ),
            DeltaExpr::Negate(e) => logical(Operation::Negate, vec![self.expression(e, expected)?]),
            DeltaExpr::Linear(inputs) => logical(
                Operation::Apply {
                    operator: self.operator.clone(),
                    mode: ApplyMode::SignedLinear,
                },
                inputs
                    .iter()
                    .zip(&self.operator.inputs)
                    .map(|(e, s)| self.expression(e, Arc::new(s.as_arrow().clone())))
                    .collect::<Result<_>>()?,
            ),
            DeltaExpr::Join { left, right } => logical(
                Operation::Apply {
                    operator: self.operator.clone(),
                    mode: ApplyMode::SignedJoin,
                },
                vec![
                    self.expression(left, Arc::new(self.operator.inputs[0].as_arrow().clone()))?,
                    self.expression(right, Arc::new(self.operator.inputs[1].as_arrow().clone()))?,
                ],
            ),
        })
    }
}

/// Compile an already verified compatibility-rule result into explicit Optd plans.
pub(crate) fn compile(
    context: &RuleContext<'_>,
    maintenance: &Maintenance,
    after_inputs: Vec<IncrementalRel>,
    extra_rules: &[Arc<PlanRule>],
) -> Result<CompiledProgram> {
    let operator = RelationOperator::from_logical(context.logical_plan())?;
    let mut compiler = Compiler {
        context,
        operator: operator.clone(),
        token: None,
    };
    let mut state = None;
    if let Maintenance::Delta(expr) = maintenance {
        if let Some(scope) = expr.key_scope() {
            let keys = super::capability::key_expressions(context.logical_plan(), scope)?
                .ok_or_else(|| Error::Invalid("missing qualified scope".into()))?;
            let projections = operator
                .inputs
                .iter()
                .zip(&keys)
                .map(|(schema, keys)| {
                    RelationOperator::projection(
                        schema.clone(),
                        keys.iter()
                            .enumerate()
                            .map(|(i, e)| e.clone().alias(format!("__key_{i}")))
                            .collect(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let output_projection = if scope == KeyScope::WindowPartitions {
                Some(RelationOperator::projection(
                    operator.template.schema().clone(),
                    keys[0]
                        .iter()
                        .enumerate()
                        .map(|(i, e)| e.clone().alias(format!("__key_{i}")))
                        .collect(),
                )?)
            } else {
                None
            };
            let spec = IndexSpec {
                id: context.node_id(),
                scope,
                inputs: context.inputs().to_vec(),
                projections,
                output_projection,
                before: expr.reads_scoped(SnapshotVersion::Before),
                after: expr.reads_scoped(SnapshotVersion::After),
            };
            let changes = spec
                .projections
                .iter()
                .enumerate()
                .map(|(port, p)| {
                    logical(
                        Operation::ProjectKeys(p.clone()),
                        vec![compiler.input(port, None)],
                    )
                })
                .collect();
            compiler.token = Some(logical(Operation::PrepareIndex(spec.clone()), changes));
            state = Some(StateSpec::Keyed(spec));
        }
    }
    let root = match maintenance {
        Maintenance::Delta(expr) => {
            let delta = compiler.expression(expr, operator.schema())?;
            if let Some(StateSpec::Keyed(spec)) = &state {
                logical(
                    Operation::CompleteIndex(spec.clone()),
                    vec![
                        compiler
                            .token
                            .clone()
                            .ok_or_else(|| Error::Invalid("missing scope token".into()))?,
                        delta,
                    ],
                )
            } else {
                delta
            }
        }
        Maintenance::ChangedGroups => {
            let LogicalPlan::Aggregate(agg) = context.logical_plan() else {
                return Err(Error::Invalid("group state without aggregate".into()));
            };
            let projection =
                RelationOperator::projection(operator.inputs[0].clone(), agg.group_expr.clone())?;
            state = Some(StateSpec::Group {
                id: context.node_id(),
                input: context.inputs()[0],
                projection: projection.clone(),
            });
            let projected = logical(
                Operation::ProjectKeys(projection),
                vec![compiler.input(0, None)],
            );
            let token = logical(
                Operation::PrepareGroups {
                    state: context.node_id(),
                },
                vec![projected],
            );
            let input = logical(
                Operation::GroupLookup {
                    state: context.node_id(),
                    schema: Arc::new(operator.inputs[0].as_arrow().clone()),
                },
                vec![token.clone()],
            );
            let output = logical(
                Operation::Apply {
                    operator: operator.clone(),
                    mode: ApplyMode::Positive,
                },
                vec![input],
            );
            logical(
                Operation::CompleteGroups {
                    state: context.node_id(),
                },
                vec![token, output],
            )
        }
        Maintenance::Recompute => {
            let inputs = if let LogicalPlan::TableScan(scan) = context.logical_plan() {
                vec![logical(
                    Operation::SourceRead {
                        name: scan.table_name.to_string(),
                        delta: false,
                        schema: scan.source.schema(),
                    },
                    vec![],
                )]
            } else {
                after_inputs
            };
            let output = logical(
                Operation::Apply {
                    operator: operator.clone(),
                    mode: ApplyMode::Positive,
                },
                inputs,
            );
            logical(
                Operation::Replace,
                vec![
                    output,
                    read(
                        context.node_id(),
                        operator.schema(),
                        Some(SnapshotVersion::Before),
                    ),
                ],
            )
        }
        Maintenance::Constant => logical(Operation::Empty(operator.schema()), vec![]),
    };
    let dummy = IncrementalNode::ScopedSnapshot {
        operator,
        scope: KeyScope::JoinKeys,
        version: SnapshotVersion::Before,
        state: 0,
    };
    let expanded = optimize(
        root,
        vec![Arc::new(ExpandScoped {
            matcher: matcher(&dummy),
        })],
        ApplyOrder::TopDown,
    )?;
    let expanded_for_bindings = expanded.clone();
    let logical = if extra_rules.is_empty() {
        expanded
    } else {
        optimize(expanded, extra_rules.to_vec(), ApplyOrder::BottomUp)?
    };
    let mut reads = std::collections::BTreeMap::new();
    fn bind_reads(node: &IncrementalRel, reads: &mut std::collections::BTreeMap<usize, SchemaRef>) {
        if let IncrementalNode::Logical(
            Operation::SnapshotRead { input, schema, .. } | Operation::DeltaRead { input, schema },
        ) = &node.typ
        {
            reads.insert(*input, schema.clone());
        }
        for child in &node.children {
            if let PlanNodeOrGroup::PlanNode(child) = child {
                bind_reads(child, reads);
            }
        }
    }
    bind_reads(&expanded_for_bindings, &mut reads);
    super::verify_plan::verify(&logical, false, context, &state, &reads)?;
    let physical = optimize(
        logical.clone(),
        vec![Arc::new(Implement {
            matcher: matcher(&IncrementalNode::Logical(Operation::Sum)),
        })],
        ApplyOrder::BottomUp,
    )?;
    super::verify_plan::verify(&physical, true, context, &state, &reads)?;
    let steps = lower_steps(&physical)?;
    Ok(CompiledProgram {
        logical,
        physical,
        state,
        steps,
    })
}

/// Compile a candidate input binding; ordered chains explicitly retain their operators.
pub(crate) fn after_input(
    id: usize,
    plan: &LogicalPlan,
    ordered: bool,
    children: Vec<IncrementalRel>,
) -> Result<IncrementalRel> {
    Ok(if ordered {
        logical(
            Operation::Apply {
                operator: RelationOperator::from_logical(plan)?,
                mode: ApplyMode::Positive,
            },
            children,
        )
    } else {
        read(
            id,
            Arc::new(plan.schema().as_arrow().clone()),
            Some(SnapshotVersion::After),
        )
    })
}
