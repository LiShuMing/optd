//! Local verifier boundaries; execution/oracle acceptance lives in integration tests.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};

use super::expr::verify;
use super::{
    DeltaExpr, IncrementalOptimizer, IncrementalPlan, Maintenance, RuleContext, MAX_EXPR_DEPTH,
    MAX_EXPR_NODES,
};
use crate::Result;

fn scan(projection: Option<Vec<usize>>) -> Result<LogicalPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
    ]));
    let batch = RecordBatch::new_empty(schema.clone());
    let provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
    Ok(LogicalPlanBuilder::scan("t", provider_as_source(provider), projection)?.build()?)
}

#[test]
fn scan_delta_has_raw_schema_and_must_pass_through_its_original_projection() -> Result<()> {
    let plan = scan(Some(vec![0]))?;
    let context = RuleContext::new(0, &plan, &[], false, false);
    let compiled = IncrementalOptimizer::default().rewrite_node(&context)?;
    assert_eq!(
        compiled.maintenance,
        Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta]))
    );
    verify(&context, &compiled.maintenance)?;
    let error = verify(&context, &Maintenance::Delta(DeltaExpr::SourceDelta))
        .expect_err("raw source and projected output have different schemas");
    assert!(error.to_string().contains("schema differs"));
    Ok(())
}

#[test]
fn fetch_scan_cannot_be_lifted_as_linear_and_has_an_explicit_rule() -> Result<()> {
    let mut plan = scan(None)?;
    // The helper creates a TableScan directly; retain a defensive test assertion.
    let LogicalPlan::TableScan(scan) = &mut plan else {
        panic!("scan helper returned another operator");
    };
    scan.fetch = Some(1);
    let context = RuleContext::new(0, &plan, &[], false, false);
    let compiled = IncrementalOptimizer::default().rewrite_node(&context)?;
    assert_eq!(compiled.maintenance, Maintenance::Recompute);
    assert_eq!(compiled.rule.name, "builtin.source.fetch_recompute");
    assert!(verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta]))
    )
    .is_err());
    Ok(())
}

#[test]
fn nested_linear_inputs_are_checked_at_each_original_typed_port() -> Result<()> {
    let plan = LogicalPlanBuilder::from(scan(None)?)
        .project(vec![col("t.y"), col("t.x")])?
        .build()?;
    let context = RuleContext::new(1, &plan, &[0], false, false);
    verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::InputDelta(0)])),
    )?;
    let nested = Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::Linear(vec![
        DeltaExpr::InputDelta(0),
    ])]));
    assert!(verify(&context, &nested).is_err());
    assert!(verify(&context, &Maintenance::Delta(DeltaExpr::InputDelta(0))).is_err());
    assert!(verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::InputDelta(1)]))
    )
    .is_err());
    assert!(verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta]))
    )
    .is_err());
    Ok(())
}

#[test]
fn expression_budgets_include_the_root_and_accept_the_exact_boundary() -> Result<()> {
    let plan = scan(None)?;
    let context = RuleContext::new(0, &plan, &[], false, false);
    let mut expression = DeltaExpr::Empty;
    for _ in 1..MAX_EXPR_DEPTH {
        expression = DeltaExpr::Negate(Box::new(expression));
    }
    verify(&context, &Maintenance::Delta(expression.clone()))?;
    assert!(verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Negate(Box::new(expression)))
    )
    .is_err());
    verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Sum(vec![DeltaExpr::Empty; MAX_EXPR_NODES - 1])),
    )?;
    assert!(verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Sum(vec![DeltaExpr::Empty; MAX_EXPR_NODES]))
    )
    .is_err());
    assert!(verify(&context, &Maintenance::Delta(DeltaExpr::Sum(vec![]))).is_err());
    Ok(())
}

#[test]
fn verified_plan_rejects_detached_nodes_forward_edges_and_noncontiguous_ids() -> Result<()> {
    let source = scan(None)?;
    let projection = LogicalPlanBuilder::from(source.clone())
        .project(vec![col("t.x")])?
        .build()?;
    let optimizer = IncrementalOptimizer::default();
    let first = optimizer.rewrite_node(&RuleContext::new(0, &source, &[], false, false))?;
    let second = optimizer.rewrite_node(&RuleContext::new(1, &projection, &[0], false, false))?;
    let plan = IncrementalPlan::new(vec![first.clone(), second.clone()])?;
    assert_eq!(plan.root(), 1);
    let explanation = plan.explain();
    assert!(explanation.contains("builtin.source.delta@1"));
    assert!(explanation.contains("Linear([SourceDelta])"));
    assert!(explanation.contains("InputDelta(0)"));

    let mut forward = second.clone();
    forward.inputs = vec![1];
    assert!(IncrementalPlan::new(vec![first.clone(), forward]).is_err());
    let mut wrong_id = second;
    wrong_id.id = 4;
    assert!(IncrementalPlan::new(vec![first.clone(), wrong_id]).is_err());
    let detached = optimizer.rewrite_node(&RuleContext::new(1, &source, &[], false, false))?;
    assert!(IncrementalPlan::new(vec![first, detached]).is_err());
    assert!(IncrementalPlan::new(vec![]).is_err());
    Ok(())
}
