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

fn keyed_join(left_type: DataType, right_type: DataType) -> Result<LogicalPlan> {
    let typed_scan = |name: &str, data_type: DataType| -> Result<LogicalPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", data_type, true)]));
        let provider = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![RecordBatch::new_empty(schema)]],
        )?);
        Ok(LogicalPlanBuilder::scan(name, provider_as_source(provider), None)?.build()?)
    };
    Ok(LogicalPlanBuilder::from(typed_scan("a", left_type)?)
        .join(
            typed_scan("b", right_type)?,
            datafusion::logical_expr::JoinType::Inner,
            (vec!["a.k"], vec!["b.k"]),
            None,
        )?
        .build()?)
}

#[test]
fn scoped_join_admission_checks_both_key_types_before_execution() -> Result<()> {
    use super::{KeyScope, SnapshotVersion};
    for (left, right, eligible) in [
        (DataType::Int64, DataType::Int64, true),
        (DataType::Utf8, DataType::Utf8, true),
        (DataType::Int32, DataType::Int64, false),
        (DataType::Float64, DataType::Float64, false),
    ] {
        // Construct the typed operator directly, so SQL cast insertion cannot
        // hide a mismatched pair of key types from this capability test.
        let plan = keyed_join(left, right)?;
        let context = RuleContext::new(2, &plan, &[0, 1], false, false);
        assert_eq!(context.supports_key_scope(KeyScope::JoinKeys)?, eligible);
        assert!(!context.supports_key_scope(KeyScope::WindowPartitions)?);
        let program = Maintenance::Delta(DeltaExpr::ScopedSnapshot {
            scope: KeyScope::JoinKeys,
            version: SnapshotVersion::After,
        });
        assert_eq!(verify(&context, &program).is_ok(), eligible);
    }
    Ok(())
}

#[test]
fn scoped_output_cannot_be_used_as_a_narrower_input_snapshot_or_ordered_bypass() -> Result<()> {
    use super::{KeyScope, SnapshotVersion};
    let plan = keyed_join(DataType::Int64, DataType::Int64)?;
    let context = RuleContext::new(2, &plan, &[0, 1], false, false);
    let snapshot = DeltaExpr::ScopedSnapshot {
        scope: KeyScope::JoinKeys,
        version: SnapshotVersion::Before,
    };
    verify(&context, &Maintenance::Delta(snapshot.clone()))?;
    let error = verify(
        &context,
        &Maintenance::Delta(DeltaExpr::Join {
            left: Box::new(snapshot.clone()),
            right: Box::new(DeltaExpr::InputDelta(1)),
        }),
    )
    .expect_err("a two-column join output is not its one-column left input");
    assert!(
        error.to_string().contains("scoped output schema differs"),
        "{error}"
    );
    let ordered = RuleContext::new(2, &plan, &[0, 1], true, false);
    assert!(!ordered.supports_key_scope(KeyScope::JoinKeys)?);
    assert!(verify(&ordered, &Maintenance::Delta(snapshot)).is_err());
    Ok(())
}

#[tokio::test]
async fn one_window_scope_requires_common_nonempty_admitted_partition_keys() -> Result<()> {
    use super::{KeyScope, SnapshotVersion};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{Expr, Window};
    use datafusion::prelude::SessionContext;

    let context = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
    ]));
    context.register_batch("t", RecordBatch::new_empty(schema))?;
    let analyzed = context
        .state()
        .create_logical_plan("SELECT ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn FROM t")
        .await?;
    let mut window = None;
    analyzed.apply(|plan| {
        if let LogicalPlan::Window(w) = plan {
            window = Some(w.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let window = window.expect("SQL has one explicit window");
    let mut different = window.window_expr[0].clone();
    let mut expression = &mut different;
    while let Expr::Alias(alias) = expression {
        expression = &mut alias.expr;
    }
    let Expr::WindowFunction(function) = expression else {
        panic!("window contains a window expression");
    };
    function.params.partition_by = vec![col("t.y")];
    let mixed = LogicalPlan::Window(Window::try_new(
        vec![
            window.window_expr[0].clone(),
            different.alias("second_window"),
        ],
        window.input.clone(),
    )?);
    let mut global = window.clone();
    let mut expression = &mut global.window_expr[0];
    while let Expr::Alias(alias) = expression {
        expression = &mut alias.expr;
    }
    let Expr::WindowFunction(function) = expression else {
        panic!("window contains a window expression");
    };
    function.params.partition_by.clear();
    for (plan, eligible) in [
        (LogicalPlan::Window(window), true),
        (mixed, false),
        (LogicalPlan::Window(global), false),
    ] {
        let ctx = RuleContext::new(1, &plan, &[0], false, false);
        assert_eq!(
            ctx.supports_key_scope(KeyScope::WindowPartitions)?,
            eligible
        );
        let expression = Maintenance::Delta(DeltaExpr::ScopedSnapshot {
            scope: KeyScope::WindowPartitions,
            version: SnapshotVersion::After,
        });
        assert_eq!(verify(&ctx, &expression).is_ok(), eligible);
        let compiled = IncrementalOptimizer::default().rewrite_node(&ctx)?;
        assert_eq!(
            compiled.rule.name,
            if eligible {
                "builtin.partitioned_window"
            } else {
                "builtin.window_recompute"
            }
        );
    }
    Ok(())
}

#[test]
fn null_aware_anti_join_cannot_declare_only_equal_key_dependencies() -> Result<()> {
    use super::{KeyScope, SnapshotVersion};
    use datafusion::logical_expr::JoinType;
    let base = keyed_join(DataType::Int64, DataType::Int64)?;
    let LogicalPlan::Join(join) = base else {
        panic!("builder creates a join");
    };
    for null_aware in [false, true] {
        let plan = LogicalPlan::Join(datafusion::logical_expr::Join::try_new(
            join.left.clone(),
            join.right.clone(),
            join.on.clone(),
            join.filter.clone(),
            JoinType::LeftAnti,
            join.join_constraint,
            join.null_equality,
            null_aware,
        )?);
        let context = RuleContext::new(2, &plan, &[0, 1], false, false);
        assert_eq!(context.supports_key_scope(KeyScope::JoinKeys)?, !null_aware);
        let program = Maintenance::Delta(DeltaExpr::ScopedSnapshot {
            scope: KeyScope::JoinKeys,
            version: SnapshotVersion::After,
        });
        assert_eq!(verify(&context, &program).is_ok(), !null_aware);
    }
    Ok(())
}
