//! Precompiled, epoch-isolated physical templates for stateless incremental operators.

use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::{concat_batches, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use crate::planner::plan::{CompiledProgram, Operation, RelationOperator};
use crate::{Error, ExecutionTemplateWork, Result};

enum UnaryStage {
    Projection {
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        output_schema: SchemaRef,
    },
    Filter {
        predicate: Arc<dyn PhysicalExpr>,
        output_schema: SchemaRef,
    },
    RewrapSchema {
        output_schema: SchemaRef,
    },
}

/// Reusable physical evaluation strategy compiled once per `SqlView` step.
///
/// Direct templates never retain batch data across epochs. Any unsupported shape
/// or type coercion falls back conservatively to DataFusion's physical planner.
pub(crate) struct CompiledOperator(OperatorKind);

enum OperatorKind {
    /// Unary chain of `Projection`, `Filter`, `SubqueryAlias`, `Repartition` and
    /// fetch-free `TableScan` templates over a single typed input port.
    DirectPipeline {
        input_schema: SchemaRef,
        output_schema: SchemaRef,
        stages: Vec<UnaryStage>,
    },
    /// Same-type multiset concatenation across multiple input ports.
    DirectUnion {
        input_schemas: Vec<SchemaRef>,
        output_schema: SchemaRef,
    },
    /// Complex or stateful operator evaluated through DataFusion's physical planner.
    Fallback(RelationOperator),
}

impl CompiledOperator {
    /// Compile a typed `RelationOperator` once against the fixed SQL context.
    pub(crate) fn compile(ctx: &SessionContext, operator: &RelationOperator) -> Result<Self> {
        let output_schema = operator.schema();
        if let Some(stages) = compile_unary_pipeline(ctx, operator)? {
            return Ok(Self(OperatorKind::DirectPipeline {
                input_schema: Arc::new(operator.inputs[0].as_arrow().clone()),
                output_schema,
                stages,
            }));
        }
        if let Some(input_schemas) = compile_direct_union(operator, &output_schema) {
            return Ok(Self(OperatorKind::DirectUnion {
                input_schemas,
                output_schema,
            }));
        }
        Ok(Self(OperatorKind::Fallback(operator.clone())))
    }

    /// Output Arrow schema guaranteed by this compiled operator.
    pub(crate) fn schema(&self) -> SchemaRef {
        match &self.0 {
            OperatorKind::DirectPipeline { output_schema, .. }
            | OperatorKind::DirectUnion { output_schema, .. } => Arc::clone(output_schema),
            OperatorKind::Fallback(operator) => operator.schema(),
        }
    }

    /// Execute the template over epoch-local input batches and record template work.
    pub(crate) async fn apply(
        &self,
        ctx: &SessionContext,
        batches: &[RecordBatch],
        work: &mut ExecutionTemplateWork,
    ) -> Result<RecordBatch> {
        match &self.0 {
            OperatorKind::DirectPipeline {
                input_schema,
                output_schema,
                stages,
            } => {
                if batches.len() != 1 {
                    return Err(Error::Invalid(
                        "physical operator input arity differs".into(),
                    ));
                }
                if batches[0].schema().as_ref() != input_schema.as_ref() {
                    return Err(Error::Invalid(
                        "physical operator input schema differs".into(),
                    ));
                }
                if batches[0].num_rows() == 0 {
                    work.record_empty_short_circuit()?;
                    return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
                }
                work.record_direct()?;
                let mut current = batches[0].clone();
                for stage in stages {
                    if current.num_rows() == 0 {
                        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
                    }
                    current = eval_stage(stage, &current)?;
                }
                Ok(current)
            }
            OperatorKind::DirectUnion {
                input_schemas,
                output_schema,
            } => {
                if batches.len() != input_schemas.len() {
                    return Err(Error::Invalid(
                        "physical operator input arity differs".into(),
                    ));
                }
                for (batch, schema) in batches.iter().zip(input_schemas) {
                    if batch.schema().as_ref() != schema.as_ref() {
                        return Err(Error::Invalid(
                            "physical operator input schema differs".into(),
                        ));
                    }
                }
                if batches.iter().all(|b| b.num_rows() == 0) {
                    work.record_empty_short_circuit()?;
                    return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
                }
                work.record_direct()?;
                let mut aligned = Vec::with_capacity(batches.len());
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    aligned.push(rewrap_schema(batch, output_schema)?);
                }
                Ok(concat_batches(output_schema, &aligned)?)
            }
            OperatorKind::Fallback(operator) => {
                work.record_fallback()?;
                super::apply(ctx, operator, batches).await
            }
        }
    }
}

/// Precompiled operator templates aligned 1-to-1 with a `CompiledProgram`'s steps.
pub(crate) struct ProgramTemplates {
    steps: Vec<Option<CompiledOperator>>,
}

impl ProgramTemplates {
    /// Compile all eligible operators in one node's physical step list.
    pub(crate) fn compile(ctx: &SessionContext, program: &CompiledProgram) -> Result<Self> {
        let mut steps = Vec::with_capacity(program.steps.len());
        for step in &program.steps {
            let compiled = match &step.operation {
                Operation::Apply { operator, .. }
                | Operation::ProjectKeys(operator)
                | Operation::EvaluateSnapshot { operator, .. } => {
                    Some(CompiledOperator::compile(ctx, operator)?)
                }
                Operation::CompleteIndex(spec) => spec
                    .output_projection
                    .as_ref()
                    .map(|projection| CompiledOperator::compile(ctx, projection))
                    .transpose()?,
                _ => None,
            };
            steps.push(compiled);
        }
        Ok(Self { steps })
    }

    /// Return the precompiled operator for a physical step index.
    pub(crate) fn step(&self, index: usize) -> Option<&CompiledOperator> {
        self.steps.get(index).and_then(Option::as_ref)
    }
}

fn eval_stage(stage: &UnaryStage, batch: &RecordBatch) -> Result<RecordBatch> {
    match stage {
        UnaryStage::Projection {
            exprs,
            output_schema,
        } => {
            let mut columns = Vec::with_capacity(exprs.len());
            for expr in exprs {
                columns.push(expr.evaluate(batch)?.into_array(batch.num_rows())?);
            }
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(output_schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )?)
        }
        UnaryStage::Filter {
            predicate,
            output_schema,
        } => {
            let evaluated = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
            let Some(mask) = evaluated.as_any().downcast_ref::<BooleanArray>() else {
                return Err(Error::Invalid(
                    "filter predicate did not evaluate to Boolean".into(),
                ));
            };
            if batch.num_columns() == 0 {
                Ok(RecordBatch::try_new_with_options(
                    Arc::clone(output_schema),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(mask.true_count())),
                )?)
            } else {
                let filtered = filter_record_batch(batch, mask)?;
                rewrap_schema(&filtered, output_schema)
            }
        }
        UnaryStage::RewrapSchema { output_schema } => rewrap_schema(batch, output_schema),
    }
}

fn rewrap_schema(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        batch.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

fn compile_unary_pipeline(
    ctx: &SessionContext,
    operator: &RelationOperator,
) -> Result<Option<Vec<UnaryStage>>> {
    if operator.inputs.len() != 1 {
        return Ok(None);
    }
    let session = ctx.state();
    let mut reversed = Vec::new();
    let mut current = operator.template.as_ref();
    loop {
        match current {
            LogicalPlan::EmptyRelation(empty) => {
                if empty.produce_one_row || empty.schema.as_ref() != operator.inputs[0].as_ref() {
                    return Ok(None);
                }
                break;
            }
            LogicalPlan::Projection(projection) => {
                let input_df = projection.input.schema();
                let output_schema = Arc::new(projection.schema.as_arrow().clone());
                if projection.expr.len() != output_schema.fields().len() {
                    return Ok(None);
                }
                let mut exprs = Vec::with_capacity(projection.expr.len());
                for (expr, field) in projection.expr.iter().zip(output_schema.fields()) {
                    let Ok(physical) = session.create_physical_expr(expr.clone(), input_df) else {
                        return Ok(None);
                    };
                    let Ok(data_type) = physical.data_type(input_df.as_arrow()) else {
                        return Ok(None);
                    };
                    if &data_type != field.data_type() {
                        return Ok(None);
                    }
                    exprs.push(physical);
                }
                reversed.push(UnaryStage::Projection {
                    exprs,
                    output_schema,
                });
                current = projection.input.as_ref();
            }
            LogicalPlan::Filter(filter) => {
                let input_df = filter.input.schema();
                let output_schema = Arc::new(filter.input.schema().as_arrow().clone());
                let Ok(predicate) =
                    session.create_physical_expr(filter.predicate.clone(), input_df)
                else {
                    return Ok(None);
                };
                let Ok(data_type) = predicate.data_type(input_df.as_arrow()) else {
                    return Ok(None);
                };
                if data_type != DataType::Boolean {
                    return Ok(None);
                }
                reversed.push(UnaryStage::Filter {
                    predicate,
                    output_schema,
                });
                current = filter.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(alias) => {
                let input_schema = alias.input.schema().as_arrow();
                let output_schema = Arc::new(alias.schema.as_arrow().clone());
                if !same_column_types(input_schema, output_schema.as_ref()) {
                    return Ok(None);
                }
                reversed.push(UnaryStage::RewrapSchema { output_schema });
                current = alias.input.as_ref();
            }
            LogicalPlan::Repartition(repartition) => {
                let output_schema = Arc::new(repartition.input.schema().as_arrow().clone());
                reversed.push(UnaryStage::RewrapSchema { output_schema });
                current = repartition.input.as_ref();
            }
            _ => return Ok(None),
        }
    }
    if reversed.is_empty() {
        reversed.push(UnaryStage::RewrapSchema {
            output_schema: operator.schema(),
        });
    }
    reversed.reverse();
    Ok(Some(reversed))
}

fn compile_direct_union(
    operator: &RelationOperator,
    output_schema: &SchemaRef,
) -> Option<Vec<SchemaRef>> {
    let LogicalPlan::Union(union) = operator.template.as_ref() else {
        return None;
    };
    if union.inputs.len() != operator.inputs.len() || union.inputs.is_empty() {
        return None;
    }
    let mut schemas = Vec::with_capacity(union.inputs.len());
    for (input, expected) in union.inputs.iter().zip(&operator.inputs) {
        let LogicalPlan::EmptyRelation(empty) = input.as_ref() else {
            return None;
        };
        if empty.produce_one_row || empty.schema.as_ref() != expected.as_ref() {
            return None;
        }
        let arrow = Arc::new(expected.as_arrow().clone());
        if !same_column_types(arrow.as_ref(), output_schema.as_ref()) {
            return None;
        }
        schemas.push(arrow);
    }
    Some(schemas)
}

fn same_column_types(
    left: &datafusion::arrow::datatypes::Schema,
    right: &datafusion::arrow::datatypes::Schema,
) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(l, r)| l.data_type() == r.data_type())
}
