//! Actual Optd transformation, physical lowering and execution contracts.

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{
    tree_node::{TreeNode, TreeNodeRecursion},
    ScalarValue,
};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use optd_core::cascades::GroupId;
use optd_core::heuristics::HeuristicsOptimizer;
use optd_core::nodes::{NodeType, PlanNode, PlanNodeOrGroup};
use optd_core::rules::{Rule, RuleMatcher};
use optd_incremental_sql::planner::plan::{
    logical, ApplyMode, IncrementalNode, IncrementalRel, Operation, StateSpec,
};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy, TableChange};
use std::collections::{BTreeMap, HashMap};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn batch(rows: &[(Option<i64>, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn tables() -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([
        (
            "a".into(),
            batch(&[(Some(1), Some(10)), (Some(2), Some(20)), (None, Some(30))]),
        ),
        (
            "b".into(),
            batch(&[
                (Some(1), Some(40)),
                (Some(1), Some(40)),
                (Some(3), Some(50)),
            ]),
        ),
    ])
}
fn walk(root: &IncrementalRel, physical: bool) {
    assert_eq!(root.typ.is_logical(), !physical);
    assert!(matches!(
        (&root.typ, physical),
        (IncrementalNode::Logical(_), false) | (IncrementalNode::Physical(_), true)
    ));
    for child in &root.children {
        let PlanNodeOrGroup::PlanNode(child) = child else {
            panic!("unresolved group")
        };
        walk(child, physical);
    }
}
fn assert_closed(operator: &optd_incremental_sql::planner::plan::RelationOperator) {
    operator
        .template
        .apply(|plan| {
            assert!(
                !matches!(plan, LogicalPlan::TableScan(_)),
                "compiled kernel retains an original provider"
            );
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
}

#[tokio::test]
async fn all_outer_semi_anti_plans_expose_index_access_and_share_one_preparation() {
    for kind in [
        "LEFT",
        "RIGHT",
        "FULL",
        "LEFT SEMI",
        "RIGHT SEMI",
        "LEFT ANTI",
        "RIGHT ANTI",
    ] {
        let sql = format!("SELECT * FROM a {kind} JOIN b ON a.k=b.k AND a.v < b.v");
        let view = SqlView::new_with_optimizer(
            &sql,
            tables(),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        for program in view.incremental_plan().programs() {
            walk(&program.logical, false);
            walk(&program.physical, true);
            let Some(StateSpec::Keyed(spec)) = &program.state else {
                continue;
            };
            assert_eq!(spec.projections.len(), 2);
            let count = |f: fn(&Operation) -> bool| {
                program.steps.iter().filter(|s| f(&s.operation)).count()
            };
            assert_eq!(
                count(|op| matches!(op, Operation::PrepareIndex(_))),
                1,
                "{sql}"
            );
            assert_eq!(count(|op| matches!(op, Operation::ProjectKeys(_))), 2);
            assert_eq!(count(|op| matches!(op, Operation::IndexLookup { .. })), 4);
            assert_eq!(
                count(|op| matches!(op, Operation::EvaluateSnapshot { .. })),
                2
            );
            assert_eq!(count(|op| matches!(op, Operation::CompleteIndex(_))), 1);
            for (id, step) in program.steps.iter().enumerate() {
                assert!(step.inputs.iter().all(|i| *i < id));
                if let Operation::EvaluateSnapshot { operator, .. } = &step.operation {
                    assert_closed(operator);
                    assert!(operator
                        .template
                        .display()
                        .to_string()
                        .contains("a.v < b.v"));
                }
            }
        }
        let explain = view.incremental_plan().explain();
        assert!(explain.contains("LogicalIndexLookup"));
        assert!(explain.contains("PhysicalIndexLookup"));
        assert!(!explain.contains("ScopedSnapshot(After)"));
    }
}

#[tokio::test]
async fn window_and_group_plans_expose_different_state_contracts() {
    for sql in [
        "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v) AS n FROM a",
        "SELECT DISTINCT k,v FROM a",
    ] {
        let view = SqlView::new_with_optimizer(
            sql,
            tables(),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let mut found = false;
        for program in view.incremental_plan().programs() {
            walk(&program.logical, false);
            walk(&program.physical, true);
            match &program.state {
                Some(StateSpec::Keyed(spec)) => {
                    found = true;
                    assert!(spec.output_projection.is_some());
                    assert_eq!(
                        program
                            .steps
                            .iter()
                            .filter(|s| matches!(s.operation, Operation::CachedOutput { .. }))
                            .count(),
                        1
                    );
                    assert_eq!(
                        program
                            .steps
                            .iter()
                            .filter(|s| matches!(s.operation, Operation::EvaluateSnapshot { .. }))
                            .count(),
                        1
                    );
                }
                Some(StateSpec::Group { .. }) => {
                    found = true;
                    for name in [
                        "ProjectKeys",
                        "PrepareGroups",
                        "GroupLookup",
                        "Apply",
                        "CompleteGroups",
                    ] {
                        assert!(program.steps.iter().any(|s| s.operation.name() == name));
                    }
                }
                None => {}
            }
        }
        assert!(found, "{sql}");
    }
}

struct ThreeTerms {
    matcher: RuleMatcher<IncrementalNode>,
    calls: Arc<AtomicUsize>,
}
impl ThreeTerms {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            matcher: any_logical(),
            calls,
        }
    }
}
fn any_logical() -> RuleMatcher<IncrementalNode> {
    RuleMatcher::MatchDiscriminant {
        typ_discriminant: std::mem::discriminant(&IncrementalNode::Logical(Operation::Sum)),
        children: vec![RuleMatcher::AnyMany],
    }
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for ThreeTerms {
    fn name(&self) -> &'static str {
        "test.expand_inner_join_to_three_terms"
    }
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        &self.matcher
    }
    fn apply(
        &self,
        _: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        if !matches!(binding.typ, IncrementalNode::Logical(Operation::Sum))
            || binding.children.len() != 2
        {
            return vec![];
        }
        let first = binding.child_rel(0);
        let second = binding.child_rel(1);
        let IncrementalNode::Logical(Operation::Apply {
            operator,
            mode: ApplyMode::SignedJoin,
        }) = &first.typ
        else {
            return vec![];
        };
        if !matches!(
            second.typ,
            IncrementalNode::Logical(Operation::Apply {
                mode: ApplyMode::SignedJoin,
                ..
            })
        ) {
            return vec![];
        }
        let after_left = second.child_rel(0);
        let delta_right = second.child_rel(1);
        let IncrementalNode::Logical(Operation::SnapshotRead { input, schema, .. }) =
            &after_left.typ
        else {
            return vec![];
        };
        let before_left = logical(
            Operation::SnapshotRead {
                input: *input,
                schema: schema.clone(),
                version: optd_incremental_sql::planner::SnapshotVersion::Before,
            },
            vec![],
        );
        let apply = |left, right| {
            logical(
                Operation::Apply {
                    operator: operator.clone(),
                    mode: ApplyMode::SignedJoin,
                },
                vec![left, right],
            )
        };
        let output = logical(
            Operation::Sum,
            vec![
                first.clone(),
                apply(before_left, delta_right.clone()),
                apply(first.child_rel(0), delta_right),
            ],
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        vec![output.into()]
    }
}
type Rows = HashMap<Vec<ScalarValue>, i64>;
fn bag(batch: &RecordBatch, signed: bool) -> Rows {
    let columns = batch.num_columns() - usize::from(signed);
    let mut rows = Rows::new();
    for i in 0..batch.num_rows() {
        let row = batch.columns()[..columns]
            .iter()
            .map(|a| ScalarValue::try_from_array(a.as_ref(), i).unwrap())
            .collect();
        let weight = if signed {
            let ScalarValue::Int64(Some(w)) =
                ScalarValue::try_from_array(batch.column(columns).as_ref(), i).unwrap()
            else {
                panic!("missing weight")
            };
            w
        } else {
            1
        };
        *rows.entry(row).or_default() += weight;
    }
    rows.retain(|_, w| *w != 0);
    rows
}
async fn oracle(sql: &str, tables: BTreeMap<String, RecordBatch>) -> RecordBatch {
    let ctx = SessionContext::new();
    for (name, input) in tables {
        ctx.register_batch(name.as_str(), input).unwrap();
    }
    let df = ctx.sql(sql).await.unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    concat_batches(&schema, &df.collect().await.unwrap()).unwrap()
}

#[tokio::test]
async fn ordinary_optd_rule_changes_executed_join_formula_and_preserves_signed_sql_results() {
    let sql = "SELECT a.k,a.v,b.v AS bv FROM a JOIN b ON a.k=b.k";
    let calls = Arc::new(AtomicUsize::new(0));
    let optimizer = IncrementalOptimizer::default()
        .require_incremental()
        .with_plan_rule(Arc::new(ThreeTerms::new(calls.clone())));
    let mut view = SqlView::new_with_optimizer(sql, tables(), optimizer)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let join = view
        .incremental_plan()
        .programs()
        .iter()
        .find(|p| {
            p.steps.iter().any(|s| {
                matches!(
                    s.operation,
                    Operation::Apply {
                        mode: ApplyMode::SignedJoin,
                        ..
                    }
                )
            })
        })
        .unwrap();
    assert_eq!(
        join.steps
            .iter()
            .filter(|s| matches!(
                s.operation,
                Operation::Apply {
                    mode: ApplyMode::SignedJoin,
                    ..
                }
            ))
            .count(),
        3
    );
    let before = bag(&view.contents().unwrap(), false);
    // Both sides change the same key; the third cross term must occur exactly once.
    let changes = [
        TableChange {
            table: "a".into(),
            rows: batch(&[(Some(1), Some(11)), (Some(2), Some(20))]),
            weights: vec![1, -1],
        },
        TableChange {
            table: "b".into(),
            rows: batch(&[(Some(1), Some(41)), (Some(1), Some(40))]),
            weights: vec![1, -1],
        },
    ];
    let expected_sources = BTreeMap::from([
        (
            "a".into(),
            batch(&[(Some(1), Some(10)), (Some(1), Some(11)), (None, Some(30))]),
        ),
        (
            "b".into(),
            batch(&[
                (Some(1), Some(40)),
                (Some(1), Some(41)),
                (Some(3), Some(50)),
            ]),
        ),
    ]);
    let expected = bag(&oracle(sql, expected_sources).await, false);
    let candidate = view.prepare(&changes).await.unwrap();
    assert_eq!(bag(candidate.contents(), false), expected);
    let mut folded = before.clone();
    for (row, w) in bag(&candidate.delta().unwrap(), true) {
        *folded.entry(row).or_default() += w;
    }
    folded.retain(|_, w| *w != 0);
    assert_eq!(folded, expected);
    assert_eq!(bag(&view.contents().unwrap(), false), before);
    view.acknowledge(candidate).unwrap();
    assert_eq!(bag(&view.contents().unwrap(), false), expected);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no runtime rule selection");
}

#[derive(Clone, Copy)]
enum Malformation {
    WrongPhase,
    WrongSchema,
    BadPort,
    MemoGroup,
    MultipleResults,
    UnboundSource,
}
struct BadRule {
    matcher: RuleMatcher<IncrementalNode>,
    kind: Malformation,
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for BadRule {
    fn name(&self) -> &'static str {
        "test.malformed_optd_rule"
    }
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        &self.matcher
    }
    fn apply(
        &self,
        _: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        match (&self.kind, &binding.typ) {
            (Malformation::WrongPhase, IncrementalNode::Logical(Operation::DeltaRead { .. })) => {
                vec![PlanNode {
                    typ: IncrementalNode::Physical(Operation::Empty(Arc::new(Schema::empty()))),
                    children: vec![],
                    predicates: vec![],
                }
                .into()]
            }
            (Malformation::WrongSchema, IncrementalNode::Logical(Operation::DeltaRead { .. })) => {
                vec![logical(Operation::Empty(Arc::new(Schema::empty())), vec![]).into()]
            }
            (
                Malformation::BadPort,
                IncrementalNode::Logical(Operation::IndexLookup {
                    state,
                    version,
                    schema,
                    ..
                }),
            ) => vec![logical(
                Operation::IndexLookup {
                    state: *state,
                    port: 999,
                    version: *version,
                    schema: schema.clone(),
                },
                vec![binding.child_rel(0)],
            )
            .into()],
            (Malformation::MemoGroup, IncrementalNode::Logical(Operation::DeltaRead { .. })) => {
                vec![PlanNodeOrGroup::Group(GroupId::default())]
            }
            (
                Malformation::MultipleResults,
                IncrementalNode::Logical(Operation::DeltaRead { .. }),
            ) => vec![binding.clone().into(), binding.into()],
            (
                Malformation::UnboundSource,
                IncrementalNode::Logical(Operation::DeltaRead { schema, .. }),
            ) => vec![logical(
                Operation::SourceRead {
                    name: "unbound".into(),
                    delta: true,
                    schema: schema.clone(),
                },
                vec![],
            )
            .into()],
            _ => vec![],
        }
    }
}

#[tokio::test]
async fn malformed_optd_rule_results_fail_during_planning_without_panicking() {
    for kind in [
        Malformation::WrongPhase,
        Malformation::WrongSchema,
        Malformation::BadPort,
        Malformation::MemoGroup,
        Malformation::MultipleResults,
        Malformation::UnboundSource,
    ] {
        let optimizer = IncrementalOptimizer::default().with_plan_rule(Arc::new(BadRule {
            matcher: any_logical(),
            kind,
        }));
        let error = SqlView::new_with_optimizer(
            "SELECT * FROM a FULL JOIN b ON a.k=b.k",
            tables(),
            optimizer,
        )
        .await
        .err()
        .expect("malformed plan must fail before bootstrap");
        let message = error.to_string();
        assert!(
            message.contains("incremental Optd plan")
                || message.contains("test.malformed_optd_rule"),
            "{message}"
        );
    }
}

#[tokio::test]
async fn ordered_recompute_has_explicit_replacement_and_retains_actual_sequence() {
    let sql = "SELECT v FROM a ORDER BY v DESC LIMIT 2";
    let mut view = SqlView::new(sql, tables()).await.unwrap();
    assert!(view
        .incremental_plan()
        .programs()
        .iter()
        .any(|p| p.strategy() == Strategy::OperatorRecompute));
    for program in view.incremental_plan().programs() {
        if program.strategy() == Strategy::OperatorRecompute {
            assert!(matches!(
                program.physical.typ,
                IncrementalNode::Physical(Operation::Replace)
            ));
        }
    }
    let candidate = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: batch(&[(Some(4), Some(99))]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    let values = candidate
        .contents()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[99, 30]);
    view.acknowledge(candidate).unwrap();
}

struct FreezeProjection {
    matcher: RuleMatcher<IncrementalNode>,
}
impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for FreezeProjection {
    fn name(&self) -> &'static str {
        "test.freeze_projection_delta"
    }
    fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
        &self.matcher
    }
    fn apply(
        &self,
        _: &HeuristicsOptimizer<IncrementalNode>,
        binding: IncrementalRel,
    ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
        let IncrementalNode::Logical(Operation::Apply {
            operator,
            mode: ApplyMode::SignedLinear,
        }) = &binding.typ
        else {
            return vec![];
        };
        if operator.schema().field(0).name() != "n" {
            return vec![];
        }
        // Deliberately non-SQL-equivalent: prove the physical runtime follows the
        // transformed program instead of secretly executing the frontend formula.
        vec![logical(Operation::Empty(operator.schema()), vec![]).into()]
    }
}

#[tokio::test]
async fn transformed_optd_program_is_executed_even_when_it_differs_from_frontend_formula() {
    let sql = "SELECT v+1 AS n FROM a";
    let optimizer = IncrementalOptimizer::default().with_plan_rule(Arc::new(FreezeProjection {
        matcher: any_logical(),
    }));
    let mut view = SqlView::new_with_optimizer(sql, tables(), optimizer)
        .await
        .unwrap();
    let before = view.contents().unwrap();
    let root = view.incremental_plan().programs().last().unwrap();
    assert!(matches!(
        root.physical.typ,
        IncrementalNode::Physical(Operation::Empty(_))
    ));
    let candidate = view
        .prepare(&[TableChange {
            table: "a".into(),
            rows: batch(&[(Some(7), Some(70))]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    assert_eq!(candidate.delta().unwrap().num_rows(), 0);
    assert_eq!(bag(candidate.contents(), false), bag(&before, false));
    view.acknowledge(candidate).unwrap();
    let full = oracle(sql, view.tables().unwrap()).await;
    assert_ne!(bag(&full, false), bag(&view.contents().unwrap(), false));
}

#[tokio::test]
async fn optd_rule_and_physical_program_are_rebuilt_after_durable_reopen() {
    let sql = "SELECT a.k,a.v,b.v AS bv FROM a JOIN b ON a.k=b.k";
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let optimizer = IncrementalOptimizer::default()
        .require_incremental()
        .with_plan_rule(Arc::new(ThreeTerms::new(calls.clone())));
    let mut durable = optd_incremental_sql::DurableSqlView::open_with_optimizer(
        dir.path(),
        sql,
        tables(),
        optimizer.clone(),
    )
    .await
    .unwrap();
    let initial = bag(&durable.view().contents().unwrap(), false);
    let changes = [
        TableChange {
            table: "a".into(),
            rows: batch(&[(Some(1), Some(12))]),
            weights: vec![1],
        },
        TableChange {
            table: "b".into(),
            rows: batch(&[(Some(1), Some(42))]),
            weights: vec![1],
        },
    ];
    durable.refresh(&changes).await.unwrap();
    assert_eq!(
        bag(&durable.view().contents().unwrap(), false),
        bag(&oracle(sql, durable.view().tables().unwrap()).await, false)
    );
    drop(durable);
    let mut durable = optd_incremental_sql::DurableSqlView::open_with_optimizer(
        dir.path(),
        sql,
        BTreeMap::new(),
        optimizer,
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(durable.view().epoch(), 1);
    let deletion = changes
        .into_iter()
        .map(|mut c| {
            c.weights = vec![-1];
            c
        })
        .collect::<Vec<_>>();
    durable.refresh(&deletion).await.unwrap();
    assert_eq!(durable.view().epoch(), 2);
    assert_eq!(bag(&durable.view().contents().unwrap(), false), initial);
    assert_eq!(
        bag(&durable.view().contents().unwrap(), false),
        bag(&oracle(sql, durable.view().tables().unwrap()).await, false)
    );
}
