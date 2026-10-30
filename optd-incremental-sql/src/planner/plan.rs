//! Explicit incremental relations on Optd's generic logical/physical plan graph.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder};
use optd_core::heuristics::HeuristicsOptimizer;
use optd_core::nodes::{ArcPlanNode, NodeType, PlanNode, PlanNodeOrGroup};
use optd_core::rules::Rule;

use super::{KeyScope, SnapshotVersion};
use crate::{Error, Result};

/// An Optd incremental plan, sharing the repository's generic plan representation.
pub type IncrementalRel = ArcPlanNode<IncrementalNode>;
/// An ordinary Optd transformation rule over incremental logical relations.
pub type PlanRule = dyn Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>>;

/// A typed operator with schema-only placeholders, independent of a SQL graph id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelationOperator {
    /// Complete expressions, SQL semantics and qualified output schema.
    pub template: Arc<LogicalPlan>,
    /// Qualified schemas of the placeholder inputs, in traversal order.
    pub inputs: Vec<DFSchemaRef>,
}

fn placeholder(schema: DFSchemaRef) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema,
    })
}

impl RelationOperator {
    /// Retain an operator's expressions, replacing every child by a typed port.
    pub fn from_logical(plan: &LogicalPlan) -> Result<Self> {
        if let LogicalPlan::TableScan(scan) = plan {
            let schema = Arc::new(DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                scan.source.schema().as_ref(),
            )?);
            let mut builder = LogicalPlanBuilder::from(placeholder(schema.clone()));
            for filter in &scan.filters {
                builder = builder.filter(filter.clone())?;
            }
            if let Some(indices) = &scan.projection {
                let columns = schema.columns();
                builder = builder.project(
                    indices
                        .iter()
                        .map(|i| Expr::Column(columns[*i].clone()))
                        .collect::<Vec<_>>(),
                )?;
            }
            if let Some(fetch) = scan.fetch {
                builder = builder.limit(0, Some(fetch))?;
            }
            return Ok(Self {
                template: Arc::new(builder.build()?),
                inputs: vec![schema],
            });
        }
        let inputs = plan
            .inputs()
            .iter()
            .map(|p| p.schema().clone())
            .collect::<Vec<_>>();
        let template = plan.with_new_exprs(
            plan.expressions(),
            inputs.iter().cloned().map(placeholder).collect(),
        )?;
        Ok(Self {
            template: Arc::new(template),
            inputs,
        })
    }

    /// A typed key projection, including any aliases required by its index.
    pub fn projection(schema: DFSchemaRef, expressions: Vec<Expr>) -> Result<Self> {
        Ok(Self {
            template: Arc::new(
                LogicalPlanBuilder::from(placeholder(schema.clone()))
                    .project(expressions)?
                    .build()?,
            ),
            inputs: vec![schema],
        })
    }

    /// Exact output relation type.
    pub fn schema(&self) -> SchemaRef {
        Arc::new(self.template.schema().as_arrow().clone())
    }
}

/// Explicit state declaration consumed by index initialization and execution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexSpec {
    /// Stable state slot in this query.
    pub id: usize,
    /// Proven affected-key closure.
    pub scope: KeyScope,
    /// Global upstream relation ids, in index-port order.
    pub inputs: Vec<usize>,
    /// Typed per-port key extraction, fixed during planning.
    pub projections: Vec<RelationOperator>,
    /// Window output-key projection, when committed output is indexed.
    pub output_projection: Option<RelationOperator>,
    /// Whether the executed program reads committed input buckets.
    pub before: bool,
    /// Whether the executed program reads candidate input buckets.
    pub after: bool,
}

/// State declarations are separate from execution operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateSpec {
    /// Retractable builtin aggregates with typed key/argument expressions.
    Accumulator {
        /// Stable state slot.
        id: usize,
        /// Upstream relation.
        input: usize,
        /// Original aggregate expressions and output schema.
        operator: RelationOperator,
    },
    /// Complete affected-key input buckets and optional window output buckets.
    Keyed(IndexSpec),
    /// Group input/output index with its typed key projection.
    Group {
        /// State slot.
        id: usize,
        /// Upstream relation.
        input: usize,
        /// Complete typed key extraction.
        projection: RelationOperator,
    },
}

/// Evaluation law for a relation operator; not a rule identity or SQL-node lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ApplyMode {
    /// Ordinary positive relations, preserving the produced row sequence.
    Positive,
    /// Jointly linear lifting over signed input relations.
    SignedLinear,
    /// Bilinear lifting, including positive negative-times-negative products.
    SignedJoin,
}

/// Logical operations and their physical implementation vocabulary.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    /// Apply signed changes to a qualified aggregate state and emit output changes.
    UpdateAccumulator {
        /// Declared accumulator state slot.
        state: usize,
        /// Exact output schema.
        schema: SchemaRef,
    },
    /// Signed changes of an explicitly bound upstream relation.
    DeltaRead { input: usize, schema: SchemaRef },
    /// An immutable committed/candidate upstream relation.
    SnapshotRead {
        input: usize,
        version: SnapshotVersion,
        schema: SchemaRef,
    },
    /// Raw named-source changes or its candidate snapshot.
    SourceRead {
        name: String,
        delta: bool,
        schema: SchemaRef,
    },
    /// A typed zero signed relation.
    Empty(SchemaRef),
    /// Same-schema signed addition.
    Sum,
    /// Reverse signed multiplicities.
    Negate,
    /// Apply a self-contained typed relation operator.
    Apply {
        operator: RelationOperator,
        mode: ApplyMode,
    },
    /// Extract keys from positive and negative input changes, preserving row alignment.
    ProjectKeys(RelationOperator),
    /// Union affected keys and prepare an isolated candidate index exactly once.
    PrepareIndex(IndexSpec),
    /// Read one complete affected input port from a prepared index version.
    IndexLookup {
        state: usize,
        port: usize,
        version: SnapshotVersion,
        schema: SchemaRef,
    },
    /// Read actual committed window output; never rerun tie-sensitive Before.
    CachedOutput { state: usize, schema: SchemaRef },
    /// Evaluate a scoped snapshot with explicit inputs and work accounting.
    EvaluateSnapshot {
        state: usize,
        version: SnapshotVersion,
        operator: RelationOperator,
    },
    /// Complete candidate input/output index state using the executed signed output.
    CompleteIndex(IndexSpec),
    /// Prepare the indexed affected groups from projected signed changes.
    PrepareGroups { state: usize },
    /// Read complete surviving inputs of the affected groups.
    GroupLookup { state: usize, schema: SchemaRef },
    /// Diff aggregate output against committed groups and complete candidate state.
    CompleteGroups { state: usize },
    /// Difference against Before while retaining After's physical row sequence.
    Replace,
}

/// Optd nodes distinguish logical operations from physical implementations.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IncrementalNode {
    /// Declarative operation, subject to logical transformations.
    Logical(Operation),
    /// A snapshot request expanded by an Optd transformation rule.
    ScopedSnapshot {
        operator: RelationOperator,
        scope: KeyScope,
        version: SnapshotVersion,
        state: usize,
    },
    /// Selected physical operation; the runtime accepts only these nodes.
    Physical(Operation),
}

impl fmt::Display for IncrementalNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Logical(op) => write!(f, "Logical{}", op.name()),
            Self::Physical(op) => write!(f, "Physical{}", op.name()),
            Self::ScopedSnapshot { version, .. } => write!(f, "ScopedSnapshot({version:?})"),
        }
    }
}
impl NodeType for IncrementalNode {
    type PredType = String;
    fn is_logical(&self) -> bool {
        !matches!(self, Self::Physical(_))
    }
}
impl Operation {
    /// Stable operator name for plan explanation and diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::UpdateAccumulator { .. } => "UpdateAccumulator",
            Self::DeltaRead { .. } => "DeltaRead",
            Self::SnapshotRead { .. } => "SnapshotRead",
            Self::SourceRead { .. } => "SourceRead",
            Self::Empty(_) => "Empty",
            Self::Sum => "Sum",
            Self::Negate => "Negate",
            Self::Apply { .. } => "Apply",
            Self::ProjectKeys(_) => "ProjectKeys",
            Self::PrepareIndex(_) => "PrepareIndex",
            Self::IndexLookup { .. } => "IndexLookup",
            Self::CachedOutput { .. } => "CachedOutput",
            Self::EvaluateSnapshot { .. } => "EvaluateSnapshot",
            Self::CompleteIndex(_) => "CompleteIndex",
            Self::PrepareGroups { .. } => "PrepareGroups",
            Self::GroupLookup { .. } => "GroupLookup",
            Self::CompleteGroups { .. } => "CompleteGroups",
            Self::Replace => "Replace",
        }
    }
}

/// Construct a materialized logical Optd node; predicates live in typed templates.
pub fn logical(operation: Operation, children: Vec<IncrementalRel>) -> IncrementalRel {
    Arc::new(PlanNode {
        typ: IncrementalNode::Logical(operation),
        children: children.into_iter().map(Into::into).collect(),
        predicates: vec![],
    })
}

/// Fully compiled logical and physical programs plus explicit state requirements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledProgram {
    /// Expanded logical plan after caller transformations.
    pub logical: IncrementalRel,
    /// Executable physical plan, verified before state initialization.
    pub physical: IncrementalRel,
    /// Explicit state requirement, independent of executor-side SQL matching.
    pub state: Option<StateSpec>,
    /// Dependency-ordered physical instructions, with shared subtrees evaluated once.
    pub steps: Vec<PhysicalStep>,
}

impl CompiledProgram {
    /// Report the behavior of the final physical program, including caller rewrites.
    pub fn strategy(&self) -> super::Strategy {
        if self
            .steps
            .iter()
            .any(|s| matches!(s.operation, Operation::Replace))
        {
            super::Strategy::OperatorRecompute
        } else {
            match self.state {
                Some(StateSpec::Accumulator { .. }) => super::Strategy::AccumulatorDelta,
                Some(StateSpec::Keyed(_))
                    if !self.steps.iter().any(|step| {
                        matches!(step.operation, Operation::EvaluateSnapshot { .. })
                    }) =>
                {
                    super::Strategy::IndexedDelta
                }
                Some(StateSpec::Keyed(_)) => super::Strategy::KeyedRecompute,
                Some(StateSpec::Group { .. }) => super::Strategy::ChangedGroupRecompute,
                None => super::Strategy::NativeDelta,
            }
        }
    }
}

/// Explain explicit operators, typed relation kernels, versioned reads and state slots.
pub fn explain(root: &IncrementalRel) -> String {
    fn visit(node: &IncrementalRel, depth: usize, output: &mut String) {
        use std::fmt::Write;
        let _ = write!(output, "{}{}", "  ".repeat(depth), node.typ);
        match &node.typ {
            IncrementalNode::Logical(op) | IncrementalNode::Physical(op) => match op {
                Operation::Apply { operator, mode } => {
                    let _ = write!(output, " mode={mode:?} {}", operator.template.display());
                }
                Operation::EvaluateSnapshot {
                    state,
                    version,
                    operator,
                } => {
                    let _ = write!(
                        output,
                        " state={state} version={version:?} {}",
                        operator.template.display()
                    );
                }
                Operation::DeltaRead { input, .. } => {
                    let _ = write!(output, " input={input}");
                }
                Operation::SnapshotRead { input, version, .. } => {
                    let _ = write!(output, " input={input} version={version:?}");
                }
                Operation::SourceRead { name, delta, .. } => {
                    let _ = write!(output, " source={name} delta={delta}");
                }
                Operation::IndexLookup {
                    state,
                    port,
                    version,
                    ..
                } => {
                    let _ = write!(output, " state={state} port={port} version={version:?}");
                }
                Operation::PrepareIndex(spec) | Operation::CompleteIndex(spec) => {
                    let _ = write!(output, " state={} scope={:?}", spec.id, spec.scope);
                }
                Operation::ProjectKeys(operator) => {
                    let _ = write!(output, " {}", operator.template.display());
                }
                _ => {}
            },
            _ => {}
        }
        output.push('\n');
        for child in &node.children {
            if let PlanNodeOrGroup::PlanNode(child) = child {
                visit(child, depth + 1, output);
            }
        }
    }
    let mut output = String::new();
    visit(root, 0, &mut output);
    output
}

/// A dependency-ordered physical operation; repeated plan subtrees share a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalStep {
    /// Fully bound physical behavior.
    pub operation: Operation,
    /// Earlier step ids in input-port order.
    pub inputs: Vec<usize>,
}

pub(super) fn lower_steps(root: &IncrementalRel) -> Result<Vec<PhysicalStep>> {
    fn add(
        node: &IncrementalRel,
        seen: &mut HashMap<IncrementalRel, usize>,
        steps: &mut Vec<PhysicalStep>,
    ) -> Result<usize> {
        if let Some(id) = seen.get(node) {
            return Ok(*id);
        }
        let IncrementalNode::Physical(operation) = &node.typ else {
            return Err(Error::Invalid(
                "physical lowering contains a logical operation".into(),
            ));
        };
        let inputs = node
            .children
            .iter()
            .map(|child| match child {
                PlanNodeOrGroup::PlanNode(child) => add(child, seen, steps),
                PlanNodeOrGroup::Group(_) => Err(Error::Invalid(
                    "physical lowering contains an unresolved memo group".into(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        let id = steps.len();
        steps.push(PhysicalStep {
            operation: operation.clone(),
            inputs,
        });
        seen.insert(node.clone(), id);
        Ok(id)
    }
    let mut steps = vec![];
    add(root, &mut HashMap::new(), &mut steps)?;
    Ok(steps)
}
