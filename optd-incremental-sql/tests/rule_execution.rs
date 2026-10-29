//! Executable custom rewrites, independent SQL deltas and reconstruction.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::planner::{
    DeltaExpr, IncrementalOptimizer, IncrementalRule, Maintenance, RuleContext, RuleRewrite,
    SnapshotVersion,
};
use optd_incremental_sql::store::CheckpointStore;
use optd_incremental_sql::{
    DurableSqlView, Error, RefreshStats, Result, SqlView, Strategy, TableChange,
};

type Row = (Option<i64>, Option<i64>);
type Sources = BTreeMap<String, Vec<Row>>;
type Rows = HashMap<Vec<ScalarValue>, i64>;
const JOIN_SQL: &str = "SELECT a.k, a.v, b.v AS bv FROM a JOIN b ON a.k=b.k";

fn batch(rows: &[Row]) -> RecordBatch {
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

fn tables(sources: &Sources) -> BTreeMap<String, RecordBatch> {
    sources
        .iter()
        .map(|(name, rows)| (name.clone(), batch(rows)))
        .collect()
}

fn change(table: &str, rows: &[Row], weights: &[i64]) -> TableChange {
    TableChange {
        table: table.into(),
        rows: batch(rows),
        weights: weights.to_vec(),
    }
}

fn row(batch: &RecordBatch, index: usize, columns: usize) -> Vec<ScalarValue> {
    batch.columns()[..columns]
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), index).unwrap())
        .collect()
}

fn add(rows: &mut Rows, row: Vec<ScalarValue>, weight: i64) {
    let next = rows
        .get(&row)
        .copied()
        .unwrap_or_default()
        .checked_add(weight)
        .unwrap();
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}

fn bag(batch: &RecordBatch) -> Rows {
    let mut rows = Rows::new();
    for index in 0..batch.num_rows() {
        add(&mut rows, row(batch, index, batch.num_columns()), 1);
    }
    rows
}

fn signed(batch: &RecordBatch) -> Rows {
    let mut rows = Rows::new();
    let columns = batch.num_columns() - 1;
    assert_eq!(batch.schema().field(columns).name(), "__optd_weight");
    for index in 0..batch.num_rows() {
        let ScalarValue::Int64(Some(weight)) =
            ScalarValue::try_from_array(batch.column(columns).as_ref(), index).unwrap()
        else {
            panic!("signed output requires nonnull Int64 weights");
        };
        assert_ne!(weight, 0);
        add(&mut rows, row(batch, index, columns), weight);
    }
    rows
}

fn apply_delta(mut before: Rows, delta: &RecordBatch) -> Rows {
    for (row, weight) in signed(delta) {
        add(&mut before, row, weight);
    }
    assert!(before.values().all(|weight| *weight > 0));
    before
}

fn apply_sources(sources: &mut Sources, changes: &[TableChange]) {
    for change in changes {
        let source = sources.get_mut(&change.table).unwrap();
        for (index, weight) in change.weights.iter().copied().enumerate() {
            let values = row(&change.rows, index, 2);
            let [ScalarValue::Int64(k), ScalarValue::Int64(v)] = values.as_slice() else {
                panic!("fixture source has two Int64 columns");
            };
            let value = (*k, *v);
            for _ in 0..weight.unsigned_abs() {
                if weight > 0 {
                    source.push(value);
                } else {
                    let found = source.iter().position(|r| *r == value).unwrap();
                    source.remove(found);
                }
            }
        }
    }
}

async fn oracle(sql: &str, sources: &Sources) -> RecordBatch {
    // This physical plan does not use the incremental planner, states or caches.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, input) in tables(sources) {
        ctx.register_batch(name.as_str(), input).unwrap();
    }
    let physical = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    concat_batches(
        &physical.schema(),
        &collect(physical, ctx.task_ctx()).await.unwrap(),
    )
    .unwrap()
}

fn assert_sources(view: &SqlView, expected: &Sources) {
    let actual = view.tables().unwrap();
    assert_eq!(actual.len(), expected.len());
    for (name, rows) in expected {
        assert_eq!(bag(&actual[name]), bag(&batch(rows)), "source {name}");
    }
}

async fn refresh_checked(
    view: &mut SqlView,
    changes: &[TableChange],
    expected_sources: &Sources,
) -> RefreshStats {
    let epoch = view.epoch();
    let before = view.contents().unwrap();
    let before_sources = view.tables().unwrap();
    let candidate = view.prepare(changes).await.unwrap();
    let expected = oracle(view.sql(), expected_sources).await;
    assert_eq!(candidate.contents().schema(), before.schema());
    assert_eq!(candidate.contents().schema(), expected.schema());
    assert_eq!(bag(candidate.contents()), bag(&expected));
    assert_eq!(
        apply_delta(bag(&before), &candidate.delta().unwrap()),
        bag(&expected)
    );
    assert_eq!(view.epoch(), epoch);
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.tables().unwrap(), before_sources);
    let stats = candidate.stats().clone();
    view.acknowledge(candidate).unwrap();
    assert_eq!(view.epoch(), epoch + 1);
    assert_sources(view, expected_sources);
    assert_eq!(bag(&view.contents().unwrap()), bag(&expected));
    stats
}

fn snapshot(input: usize, version: SnapshotVersion) -> DeltaExpr {
    DeltaExpr::InputSnapshot { input, version }
}

fn join(left: DeltaExpr, right: DeltaExpr) -> DeltaExpr {
    DeltaExpr::Join {
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn reverse() -> DeltaExpr {
    DeltaExpr::Sum(vec![
        join(
            snapshot(0, SnapshotVersion::Before),
            DeltaExpr::InputDelta(1),
        ),
        join(
            DeltaExpr::InputDelta(0),
            snapshot(1, SnapshotVersion::After),
        ),
    ])
}

fn three_terms() -> DeltaExpr {
    DeltaExpr::Sum(vec![
        join(
            DeltaExpr::InputDelta(0),
            snapshot(1, SnapshotVersion::Before),
        ),
        join(
            snapshot(0, SnapshotVersion::Before),
            DeltaExpr::InputDelta(1),
        ),
        join(DeltaExpr::InputDelta(0), DeltaExpr::InputDelta(1)),
    ])
}

#[derive(Clone, Copy)]
enum Mode {
    Reverse,
    ThreeTerms,
    Empty,
    EmptyJoinLeft,
    EmptyJoinRight,
    LinearEmpty,
    Negate,
    Recompute,
}

struct CustomRule(Mode);

impl IncrementalRule for CustomRule {
    fn name(&self) -> &str {
        match self.0 {
            Mode::Reverse => "test.reverse-join",
            Mode::ThreeTerms => "test.three-term-join",
            Mode::Empty => "test.intentional-empty",
            Mode::EmptyJoinLeft => "test.intentional-empty-left-port",
            Mode::EmptyJoinRight => "test.intentional-empty-right-port",
            Mode::LinearEmpty => "test.intentional-empty-linear-port",
            Mode::Negate => "test.intentional-negate",
            Mode::Recompute => "test.explicit-recompute",
        }
    }

    fn priority(&self) -> i32 {
        i32::MAX
    }

    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        Ok(match context.logical_plan() {
            LogicalPlan::Join(j) => {
                j.join_type == JoinType::Inner && !matches!(self.0, Mode::LinearEmpty)
            }
            LogicalPlan::Aggregate(_) => {
                matches!(self.0, Mode::Recompute) && context.group_eligible()
            }
            LogicalPlan::Projection(_) => {
                matches!(self.0, Mode::Recompute | Mode::LinearEmpty) && !context.ordered()
            }
            _ => false,
        })
    }

    fn rewrite(&self, context: &RuleContext<'_>) -> Result<RuleRewrite> {
        Ok(RuleRewrite {
            maintenance: match self.0 {
                Mode::Reverse => Maintenance::Delta(reverse()),
                Mode::ThreeTerms => Maintenance::Delta(three_terms()),
                Mode::Empty => Maintenance::Delta(DeltaExpr::Empty),
                Mode::EmptyJoinLeft => Maintenance::Delta(join(
                    DeltaExpr::Sum(vec![DeltaExpr::Empty]),
                    DeltaExpr::InputDelta(1),
                )),
                Mode::EmptyJoinRight => Maintenance::Delta(join(
                    DeltaExpr::InputDelta(0),
                    DeltaExpr::Sum(vec![DeltaExpr::Empty]),
                )),
                Mode::LinearEmpty => {
                    let inputs = context.logical_plan().inputs();
                    assert_eq!(inputs.len(), 1);
                    assert_eq!(inputs[0].schema().as_arrow().fields().len(), 2);
                    assert_eq!(context.logical_plan().schema().as_arrow().fields().len(), 1);
                    Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::Empty]))
                }
                Mode::Negate => Maintenance::Delta(DeltaExpr::Negate(Box::new(reverse()))),
                Mode::Recompute => Maintenance::Recompute,
            },
            reason: format!("independently registered {}", self.name()),
        })
    }
}

fn optimizer(mode: Mode) -> IncrementalOptimizer {
    IncrementalOptimizer::default()
        .with_rule(Arc::new(CustomRule(mode)))
        .unwrap()
}

fn join_sources() -> Sources {
    BTreeMap::from([
        (
            "a".into(),
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(2), Some(20)),
                (None, Some(7)),
            ],
        ),
        (
            "b".into(),
            vec![(Some(1), Some(100)), (Some(2), None), (None, Some(8))],
        ),
    ])
}

fn two_sided_changes() -> Vec<TableChange> {
    vec![
        change(
            "a",
            &[
                (Some(1), Some(10)),
                (Some(1), Some(11)),
                (Some(3), Some(30)),
            ],
            &[-1, 1, 1],
        ),
        change(
            "b",
            &[
                (Some(1), Some(100)),
                (Some(1), Some(101)),
                (Some(3), Some(300)),
            ],
            &[-1, 1, 1],
        ),
    ]
}

#[tokio::test]
async fn default_linear_expression_structure_executes_typed_signed_changes() {
    let sql = "SELECT k + 1 AS shifted, v FROM a WHERE v >= 0 UNION ALL SELECT k + 1 AS shifted, v FROM b WHERE v >= 0";
    let mut sources = join_sources();
    let mut view = SqlView::new(sql, tables(&sources)).await.unwrap();
    for family in ["projection", "filter", "union"] {
        let selected = view
            .incremental_plan()
            .nodes()
            .iter()
            .filter(|node| node.operator.to_lowercase().starts_with(family))
            .collect::<Vec<_>>();
        assert!(!selected.is_empty(), "missing {family}");
        for node in selected {
            assert!(!node.inputs.is_empty());
            assert_eq!(
                node.maintenance,
                Maintenance::Delta(DeltaExpr::Linear(
                    (0..node.inputs.len()).map(DeltaExpr::InputDelta).collect()
                ))
            );
        }
    }
    let scans = view
        .incremental_plan()
        .nodes()
        .iter()
        .filter(|node| node.operator.to_lowercase().starts_with("tablescan"))
        .collect::<Vec<_>>();
    assert_eq!(scans.len(), sources.len());
    for scan in scans {
        // A named raw source is a synthetic execution input, not a graph child.
        assert!(scan.inputs.is_empty());
        assert_eq!(
            scan.maintenance,
            Maintenance::Delta(DeltaExpr::Linear(vec![DeltaExpr::SourceDelta]))
        );
    }
    assert_eq!(
        bag(&view.contents().unwrap()),
        bag(&oracle(sql, &sources).await)
    );
    for changes in [
        two_sided_changes(),
        vec![
            change("a", &[(Some(4), Some(40))], &[2]),
            change("b", &[(Some(2), None), (None, Some(9))], &[-1, 1]),
        ],
        vec![change("a", &[(Some(4), Some(40))], &[-2])],
    ] {
        apply_sources(&mut sources, &changes);
        refresh_checked(&mut view, &changes, &sources).await;
    }
}

#[tokio::test]
async fn custom_reverse_join_plan_matches_full_sql_for_two_sided_and_null_duplicate_changes() {
    let mut sources = join_sources();
    let mut view =
        SqlView::new_with_optimizer(JOIN_SQL, tables(&sources), optimizer(Mode::Reverse))
            .await
            .unwrap();
    let node = view
        .incremental_plan()
        .nodes()
        .iter()
        .find(|node| node.rule.name == CustomRule(Mode::Reverse).name())
        .unwrap();
    assert_eq!(node.inputs.len(), 2);
    assert_eq!(node.maintenance, Maintenance::Delta(reverse()));
    let explanation = view.incremental_plan().explain();
    assert!(explanation.contains("test.reverse-join"));
    assert!(explanation.contains("Before"));
    assert!(explanation.contains("After"));
    for changes in [
        two_sided_changes(),
        vec![
            change("a", &[(Some(1), Some(10)), (None, Some(9))], &[-1, 1]),
            change("b", &[(Some(1), Some(101)), (None, Some(10))], &[-1, 1]),
        ],
    ] {
        apply_sources(&mut sources, &changes);
        let stats = refresh_checked(&mut view, &changes, &sources).await;
        assert!(stats.nodes.iter().any(|node| {
            node.rule.name == "test.reverse-join" && node.strategy == Strategy::NativeDelta
        }));
    }
}

#[tokio::test]
async fn three_term_join_multiplies_two_negative_changes_into_one_positive_cross_term() {
    let mut sources = BTreeMap::from([
        ("a".into(), vec![(Some(1), Some(10)); 2]),
        ("b".into(), vec![(Some(1), Some(100)); 2]),
    ]);
    let mut view =
        SqlView::new_with_optimizer(JOIN_SQL, tables(&sources), optimizer(Mode::ThreeTerms))
            .await
            .unwrap();
    assert_eq!(view.contents().unwrap().num_rows(), 4);
    assert!(view
        .incremental_plan()
        .nodes()
        .iter()
        .any(|node| node.maintenance == Maintenance::Delta(three_terms())));
    let changes = vec![
        change("a", &[(Some(1), Some(10))], &[-1]),
        change("b", &[(Some(1), Some(100))], &[-1]),
    ];
    let candidate = view.prepare(&changes).await.unwrap();
    assert_eq!(
        signed(&candidate.delta().unwrap())
            .values()
            .copied()
            .collect::<Vec<_>>(),
        vec![-3]
    );
    assert_eq!(candidate.contents().num_rows(), 1);
    drop(candidate);
    apply_sources(&mut sources, &changes);
    refresh_checked(&mut view, &changes, &sources).await;
    let changes = vec![
        change("a", &[(Some(1), Some(10))], &[1]),
        change("b", &[(Some(1), Some(100))], &[1]),
    ];
    apply_sources(&mut sources, &changes);
    refresh_checked(&mut view, &changes, &sources).await;
    assert_eq!(view.contents().unwrap().num_rows(), 4);
}

#[tokio::test]
async fn custom_self_join_shares_one_source_epoch_and_preserves_duplicate_counts() {
    let sql = "SELECT a.k, a.v AS av, b.v AS bv FROM t a JOIN t b ON a.k=b.k WHERE a.v < b.v";
    let mut sources = BTreeMap::from([(
        "t".into(),
        vec![
            (Some(1), Some(10)),
            (Some(1), Some(10)),
            (Some(1), Some(20)),
            (Some(2), Some(5)),
            (None, Some(30)),
        ],
    )]);
    let mut view = SqlView::new_with_optimizer(sql, tables(&sources), optimizer(Mode::Reverse))
        .await
        .unwrap();
    let selected = view
        .incremental_plan()
        .nodes()
        .iter()
        .filter(|node| node.rule.name == "test.reverse-join")
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].inputs.len(), 2);
    assert_eq!(selected[0].maintenance, Maintenance::Delta(reverse()));
    assert_eq!(view.contents().unwrap().num_rows(), 2);
    for changes in [
        vec![change(
            "t",
            &[
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(1), Some(30)),
                (Some(2), Some(15)),
            ],
            &[-1, -1, 1, 1],
        )],
        vec![change(
            "t",
            &[(Some(1), Some(10)), (Some(1), Some(30))],
            &[-1, 1],
        )],
    ] {
        apply_sources(&mut sources, &changes);
        let stats = refresh_checked(&mut view, &changes, &sources).await;
        let dirty_join = stats
            .nodes
            .iter()
            .filter(|node| node.rule.name == "test.reverse-join")
            .collect::<Vec<_>>();
        assert_eq!(dirty_join.len(), 1);
        assert_eq!(dirty_join[0].strategy, Strategy::NativeDelta);
        assert_eq!(view.tables().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn deliberate_empty_and_negated_rules_control_execution_instead_of_only_labels() {
    // These intentionally false identities prove execution authority. They are
    // not accepted SQL implementations and must disagree with the SQL oracle.
    let mut sources = BTreeMap::from([
        ("a".into(), vec![(Some(1), Some(10))]),
        ("b".into(), vec![(Some(1), Some(100))]),
    ]);
    let mut view = SqlView::new_with_optimizer(JOIN_SQL, tables(&sources), optimizer(Mode::Empty))
        .await
        .unwrap();
    let before = view.contents().unwrap();
    let changes = vec![
        change("a", &[(Some(2), Some(20))], &[1]),
        change("b", &[(Some(2), Some(200))], &[1]),
    ];
    apply_sources(&mut sources, &changes);
    let expected = oracle(JOIN_SQL, &sources).await;
    let candidate = view.prepare(&changes).await.unwrap();
    assert_eq!(candidate.delta().unwrap().num_rows(), 0);
    assert_eq!(bag(candidate.contents()), bag(&before));
    assert_ne!(bag(candidate.contents()), bag(&expected));
    view.acknowledge(candidate).unwrap();
    assert_sources(&view, &sources);
    let empty = BTreeMap::from([("a".into(), vec![]), ("b".into(), vec![])]);
    let view = SqlView::new_with_optimizer(JOIN_SQL, tables(&empty), optimizer(Mode::Negate))
        .await
        .unwrap();
    let before = view.contents().unwrap();
    let before_sources = view.tables().unwrap();
    assert!(view.prepare(&changes).await.is_err());
    assert_eq!(view.epoch(), 0);
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.tables().unwrap(), before_sources);
}

#[tokio::test]
async fn nested_empty_uses_typed_input_ports_and_controls_actual_execution() {
    // These deliberately incorrect identities test typed interpreter ports and
    // execution authority. Their unchanged outputs must fail the SQL oracle.
    for mode in [Mode::EmptyJoinLeft, Mode::EmptyJoinRight, Mode::LinearEmpty] {
        let (sql, mut sources, changes, expression) = match mode {
            Mode::EmptyJoinLeft | Mode::EmptyJoinRight => (
                JOIN_SQL,
                BTreeMap::from([
                    ("a".into(), vec![(Some(1), Some(10))]),
                    ("b".into(), vec![(Some(1), Some(100))]),
                ]),
                vec![
                    change("a", &[(Some(2), Some(20))], &[1]),
                    change("b", &[(Some(2), Some(200))], &[1]),
                ],
                if matches!(mode, Mode::EmptyJoinLeft) {
                    join(
                        DeltaExpr::Sum(vec![DeltaExpr::Empty]),
                        DeltaExpr::InputDelta(1),
                    )
                } else {
                    join(
                        DeltaExpr::InputDelta(0),
                        DeltaExpr::Sum(vec![DeltaExpr::Empty]),
                    )
                },
            ),
            Mode::LinearEmpty => (
                "SELECT k + v AS adjusted FROM t WHERE k > 0",
                BTreeMap::from([("t".into(), vec![(Some(1), Some(10))])]),
                vec![change("t", &[(Some(2), Some(20))], &[1])],
                DeltaExpr::Linear(vec![DeltaExpr::Empty]),
            ),
            _ => unreachable!("only the three nested-Empty fixtures enter this loop"),
        };
        let mut view = SqlView::new_with_optimizer(sql, tables(&sources), optimizer(mode))
            .await
            .unwrap();
        let name = CustomRule(mode).name().to_owned();
        let selected = view
            .incremental_plan()
            .nodes()
            .iter()
            .filter(|node| node.rule.name == name)
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].maintenance, Maintenance::Delta(expression));
        let before = view.contents().unwrap();
        // Every raw/child relation has two fields, whereas the current output
        // has one (projection) or three (join). The join's Sum combines Empty
        // with a typed child accumulator before an empty join can short-circuit.
        assert_ne!(before.num_columns(), 2);
        apply_sources(&mut sources, &changes);
        let expected = oracle(sql, &sources).await;
        let candidate = view.prepare(&changes).await.unwrap();
        assert_eq!(candidate.contents().schema(), before.schema());
        assert_eq!(candidate.delta().unwrap().num_rows(), 0);
        assert_eq!(bag(candidate.contents()), bag(&before));
        assert_ne!(bag(candidate.contents()), bag(&expected));
        assert!(candidate
            .stats()
            .nodes
            .iter()
            .any(|node| { node.rule.name == name && node.strategy == Strategy::NativeDelta }));
        view.acknowledge(candidate).unwrap();
        assert_sources(&view, &sources);
    }
}

#[tokio::test]
async fn custom_recompute_overrides_native_join_and_qualified_group_work() {
    let sql = "SELECT a.k, SUM(a.v * b.v) AS total FROM a JOIN b ON a.k=b.k GROUP BY a.k";
    let mut sources = join_sources();
    let mut view = SqlView::new_with_optimizer(sql, tables(&sources), optimizer(Mode::Recompute))
        .await
        .unwrap();
    let selected = view
        .incremental_plan()
        .nodes()
        .iter()
        .filter(|node| node.rule.name == "test.explicit-recompute")
        .collect::<Vec<_>>();
    assert!(selected
        .iter()
        .any(|node| node.operator.to_lowercase().contains("aggregate")));
    assert!(selected
        .iter()
        .any(|node| node.operator.to_lowercase().contains("join")));
    assert!(selected
        .iter()
        .all(|node| node.maintenance == Maintenance::Recompute));
    let changes = two_sided_changes();
    apply_sources(&mut sources, &changes);
    let stats = refresh_checked(&mut view, &changes, &sources).await;
    let selected = stats
        .nodes
        .iter()
        .filter(|node| node.rule.name == "test.explicit-recompute")
        .collect::<Vec<_>>();
    assert!(selected
        .iter()
        .any(|node| node.operator.to_lowercase().contains("aggregate")));
    assert!(selected
        .iter()
        .all(|node| { node.strategy == Strategy::OperatorRecompute && node.group_work.is_none() }));
}

#[tokio::test]
async fn custom_plan_rebuilds_after_durable_reopen_and_replays_each_committed_epoch() {
    let sql = "SELECT a.k, SUM(a.v * b.v) AS total FROM a JOIN b ON a.k=b.k GROUP BY a.k ORDER BY total DESC, a.k ASC LIMIT 2";
    let root = tempfile::tempdir().unwrap();
    let mut sources = join_sources();
    let mut expected_sources = vec![sources.clone()];
    let mut view = DurableSqlView::open_with_optimizer(
        root.path(),
        sql,
        tables(&sources),
        optimizer(Mode::Reverse),
    )
    .await
    .unwrap();
    let changes = two_sided_changes();
    apply_sources(&mut sources, &changes);
    view.refresh(&changes).await.unwrap();
    assert_eq!(view.view().contents().unwrap(), oracle(sql, &sources).await);
    expected_sources.push(sources.clone());
    drop(view);
    let mut view = DurableSqlView::open_with_optimizer(
        root.path(),
        sql,
        BTreeMap::new(),
        optimizer(Mode::Reverse),
    )
    .await
    .unwrap();
    assert_eq!(view.view().epoch(), 1);
    assert!(view.view().incremental_plan().nodes().iter().any(|node| {
        node.rule.name == "test.reverse-join" && node.maintenance == Maintenance::Delta(reverse())
    }));
    let changes = vec![
        change("a", &[(Some(3), Some(30)), (Some(4), Some(40))], &[-1, 1]),
        change("b", &[(Some(3), Some(300)), (Some(4), Some(400))], &[-1, 1]),
    ];
    apply_sources(&mut sources, &changes);
    view.refresh(&changes).await.unwrap();
    assert_sources(view.view(), &sources);
    assert_eq!(view.view().contents().unwrap(), oracle(sql, &sources).await);
    expected_sources.push(sources.clone());
    drop(view);
    // An equivalent default implementation may reconstruct the same protocol;
    // custom rule identities are not persisted in formats 1/2.
    let mut view = DurableSqlView::open(root.path(), sql, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(view.view().epoch(), 2);
    let changes = vec![change("b", &[(Some(4), Some(400))], &[-1])];
    apply_sources(&mut sources, &changes);
    view.refresh(&changes).await.unwrap();
    assert_eq!(view.view().contents().unwrap(), oracle(sql, &sources).await);
    expected_sources.push(sources);
    drop(view);
    let store = CheckpointStore::open(root.path()).unwrap();
    let history = store.history().unwrap();
    assert_eq!(history.len(), expected_sources.len());
    let mut replayed = Rows::new();
    for (epoch, (snapshot, sources)) in history.iter().zip(expected_sources).enumerate() {
        let expected = oracle(sql, &sources).await;
        assert_eq!(snapshot.epoch, epoch as u64);
        assert_eq!(snapshot.output, expected);
        for (name, input) in tables(&sources) {
            assert_eq!(bag(&snapshot.tables[&name]), bag(&input));
        }
        replayed = apply_delta(replayed, &snapshot.delta);
        assert_eq!(replayed, bag(&expected));
    }
}

struct FetchObserver(Arc<AtomicUsize>);

impl IncrementalRule for FetchObserver {
    fn name(&self) -> &str {
        "test.fetch-observer"
    }
    fn priority(&self) -> i32 {
        i32::MAX
    }
    fn matches(&self, context: &RuleContext<'_>) -> Result<bool> {
        if matches!(context.logical_plan(), LogicalPlan::TableScan(scan) if scan.fetch.is_some()) {
            self.0.store(context.node_id() + 1, Ordering::SeqCst);
        }
        Ok(false)
    }
    fn rewrite(&self, _context: &RuleContext<'_>) -> Result<RuleRewrite> {
        Err(Error::Invalid("nonmatching observer cannot rewrite".into()))
    }
}

#[tokio::test]
async fn fetched_scan_recomputes_candidate_source_after_changes_and_durable_reopen() {
    // Equal duplicate values remove the arbitrary-membership ambiguity of an
    // unordered LIMIT while still exposing an incorrect signed scan shortcut.
    let sql = "SELECT k, v FROM t LIMIT 2";
    let root = tempfile::tempdir().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let optimizer = IncrementalOptimizer::default()
        .with_rule(Arc::new(FetchObserver(seen.clone())))
        .unwrap();
    let value = (Some(1), Some(10));
    let mut sources = BTreeMap::from([("t".into(), vec![value; 3])]);
    let mut view =
        DurableSqlView::open_with_optimizer(root.path(), sql, tables(&sources), optimizer.clone())
            .await
            .unwrap();
    let id = seen
        .load(Ordering::SeqCst)
        .checked_sub(1)
        .expect("the optimized SQL must exercise an actual fetched TableScan");
    let scan = view
        .view()
        .incremental_plan()
        .nodes()
        .iter()
        .find(|node| node.id == id)
        .unwrap();
    assert_eq!(scan.maintenance, Maintenance::Recompute);
    for changes in [
        vec![change("t", &[value], &[-2])],
        vec![change("t", &[value], &[3])],
    ] {
        apply_sources(&mut sources, &changes);
        view.refresh(&changes).await.unwrap();
        assert_sources(view.view(), &sources);
        assert_eq!(
            bag(&view.view().contents().unwrap()),
            bag(&oracle(sql, &sources).await)
        );
    }
    drop(view);
    seen.store(0, Ordering::SeqCst);
    let mut view =
        DurableSqlView::open_with_optimizer(root.path(), sql, BTreeMap::new(), optimizer)
            .await
            .unwrap();
    assert!(seen.load(Ordering::SeqCst) > 0);
    assert_eq!(view.view().epoch(), 2);
    for changes in [
        vec![change("t", &[value], &[-4])],
        vec![change("t", &[value], &[3])],
    ] {
        apply_sources(&mut sources, &changes);
        view.refresh(&changes).await.unwrap();
        assert_sources(view.view(), &sources);
        assert_eq!(
            bag(&view.view().contents().unwrap()),
            bag(&oracle(sql, &sources).await)
        );
    }
}
