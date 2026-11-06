//! Product fragments for a planner-qualified scalar join, without outer extension.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::planner::plan::RelationOperator;
use crate::{Error, Result};

/// Execute the product selected by the planner, preserving the original outer schema.
/// Delta fragments need not contain the scalar row; an empty fragment emits no rows.
pub(super) async fn apply(
    ctx: &SessionContext,
    operator: &RelationOperator,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    let LogicalPlan::Join(join) = operator.template.as_ref() else {
        return Err(Error::Invalid(
            "scalar product kernel requires a join".into(),
        ));
    };
    let mut product = join.clone();
    product.join_type = JoinType::Inner;
    let kernel = RelationOperator {
        template: Arc::new(LogicalPlan::Join(product)),
        inputs: operator.inputs.clone(),
    };
    let output = super::apply(ctx, &kernel, batches).await?;
    // The product may strengthen field nullability. Parent ports retain the
    // original Left Join contract, including qualifiers and nullable RHS fields.
    Ok(RecordBatch::try_new_with_options(
        operator.schema(),
        output.columns().to_vec(),
        &datafusion::arrow::array::RecordBatchOptions::new()
            .with_row_count(Some(output.num_rows())),
    )?)
}
