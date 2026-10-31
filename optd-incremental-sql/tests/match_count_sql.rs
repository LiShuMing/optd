//! Match-count rules declare a physical operation rather than scoped snapshot evaluation.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::SqlView;

fn batch(rows: &[(Option<i64>, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn tables() -> BTreeMap<String, RecordBatch> {
    BTreeMap::from([
        (
            "a".into(),
            batch(&[
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(2), None),
                (None, Some(30)),
            ]),
        ),
        (
            "b".into(),
            batch(&[(Some(1), Some(40)), (Some(3), Some(50)), (None, Some(60))]),
        ),
    ])
}

#[tokio::test]
async fn pure_outer_semi_anti_plans_use_explicit_match_counts_when_enabled() {
    for kind in [
        "LEFT",
        "RIGHT",
        "FULL",
        "LEFT SEMI",
        "RIGHT SEMI",
        "LEFT ANTI",
        "RIGHT ANTI",
    ] {
        for condition in [
            "a.k=b.k",
            "a.k IS NOT DISTINCT FROM b.k",
            "a.k=b.k AND a.v=b.v",
        ] {
            let sql = format!("SELECT * FROM a {kind} JOIN b ON {condition}");
            let optimizer = IncrementalOptimizer::default()
                .with_match_count_joins()
                .unwrap()
                .require_incremental();
            let view = SqlView::new_with_optimizer(&sql, tables(), optimizer)
                .await
                .unwrap();
            let programs = view
                .incremental_plan()
                .nodes()
                .iter()
                .zip(view.incremental_plan().programs())
                .filter(|(node, _)| node.operator.contains(" Join:"))
                .collect::<Vec<_>>();
            assert_eq!(programs.len(), 1, "{sql}");
            let (node, program) = programs[0];
            assert_eq!(node.rule.name, "builtin.match_count_join", "{sql}");
            assert_eq!(
                program.strategy(),
                optd_incremental_sql::Strategy::MatchCountDelta
            );
            for name in ["PrepareIndex", "MaintainMatchJoin", "CompleteIndex"] {
                assert_eq!(
                    program
                        .steps
                        .iter()
                        .filter(|s| s.operation.name() == name)
                        .count(),
                    1,
                    "{sql}"
                );
            }

            assert!(
                program
                    .steps
                    .iter()
                    .any(|step| step.operation.name() == "MaintainMatchJoin"),
                "{sql}"
            );
            assert!(
                !program
                    .steps
                    .iter()
                    .any(|step| step.operation.name() == "EvaluateSnapshot"),
                "{sql}"
            );
        }
    }
}

type Row = (Option<i64>, Option<i64>);
type Rows = std::collections::HashMap<Vec<Option<i64>>, i64>;
const KINDS: [&str; 7] = [
    "LEFT",
    "RIGHT",
    "FULL",
    "LEFT SEMI",
    "RIGHT SEMI",
    "LEFT ANTI",
    "RIGHT ANTI",
];

fn optimizer() -> IncrementalOptimizer {
    IncrementalOptimizer::default()
        .with_match_count_joins()
        .unwrap()
        .require_incremental()
}
fn add(rows: &mut Rows, row: Vec<Option<i64>>, weight: i64) {
    let next = rows.get(&row).copied().unwrap_or_default() + weight;
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}
fn values(batch: &RecordBatch, signed: bool) -> Rows {
    let columns = batch.num_columns() - usize::from(signed);
    let mut result = Rows::new();
    for i in 0..batch.num_rows() {
        use datafusion::arrow::array::Array;
        let row = batch.columns()[..columns]
            .iter()
            .map(|a| {
                let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
                (!a.is_null(i)).then(|| a.value(i))
            })
            .collect();
        let weight = if signed {
            batch
                .column(columns)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        } else {
            1
        };
        add(&mut result, row, weight);
    }
    result
}

/// Independent integer/NULL bag oracle; SQL FALSE and UNKNOWN do not match.
/// Modes: ordinary key, NULL-safe key, composite, ordinary less-than residual,
/// NULL-safe less-than residual, NULL-aware residual expression, arithmetic residual.
fn expected(kind: &str, mode: usize, a: &[Row], b: &[Row]) -> Rows {
    let equal = |x: Option<i64>, y: Option<i64>| x.is_some() && y.is_some() && x == y;
    let matches = |x: &Row, y: &Row| {
        let key = if [1, 4].contains(&mode) {
            x.0 == y.0
        } else {
            equal(x.0, y.0)
        };
        key && match mode {
            2 => equal(x.1, y.1),
            3 | 4 => matches!((x.1,y.1),(Some(a),Some(b)) if a<b),
            5 => x.1.is_none() || matches!((x.1,y.1),(Some(a),Some(b)) if a<b),
            6 => matches!((x.1,y.1),(Some(a),Some(b)) if a+b>1),
            _ => true,
        }
    };
    let mut rows = Rows::new();
    let mut right_matched = vec![false; b.len()];
    for x in a {
        let mut matched = false;
        for (i, y) in b.iter().enumerate() {
            if matches(x, y) {
                matched = true;
                right_matched[i] = true;
                if ["LEFT", "RIGHT", "FULL"].contains(&kind) {
                    add(&mut rows, vec![x.0, x.1, y.0, y.1], 1);
                }
            }
        }
        if (kind == "LEFT SEMI" && matched) || (kind == "LEFT ANTI" && !matched) {
            add(&mut rows, vec![x.0, x.1], 1);
        }
        if !matched && ["LEFT", "FULL"].contains(&kind) {
            add(&mut rows, vec![x.0, x.1, None, None], 1);
        }
    }
    for (y, matched) in b.iter().zip(right_matched) {
        if (kind == "RIGHT SEMI" && matched) || (kind == "RIGHT ANTI" && !matched) {
            add(&mut rows, vec![y.0, y.1], 1);
        }
        if !matched && ["RIGHT", "FULL"].contains(&kind) {
            add(&mut rows, vec![None, None, y.0, y.1], 1);
        }
    }
    rows
}

async fn full(sql: &str, data: &[Vec<Row>; 2]) -> RecordBatch {
    use datafusion::prelude::{SessionConfig, SessionContext};
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, rows) in ["a", "b"].iter().zip(data) {
        ctx.register_batch(*name, batch(rows)).unwrap();
    }
    let frame = ctx.sql(sql).await.unwrap();
    let schema = frame.schema().as_arrow().clone();
    datafusion::arrow::compute::concat_batches(&Arc::new(schema), &frame.collect().await.unwrap())
        .unwrap()
}

#[tokio::test]
async fn two_sided_signed_epochs_match_independent_and_fresh_sql_oracles() {
    for kind in KINDS {
        for (mode, condition) in [
            "a.k=b.k",
            "a.k IS NOT DISTINCT FROM b.k",
            "a.k=b.k AND a.v=b.v",
            "a.k=b.k AND a.v<b.v",
            "(a.k IS NOT DISTINCT FROM b.k) AND a.v<b.v",
            "a.k=b.k AND (a.v IS NULL OR a.v<b.v)",
            "a.k=b.k AND a.v+b.v>1",
        ]
        .iter()
        .enumerate()
        {
            let sql = format!("SELECT * FROM a {kind} JOIN b ON {condition}");
            let mut data = [
                vec![(Some(1), Some(1)), (Some(1), Some(1)), (None, None)],
                vec![(Some(2), Some(2)), (None, None)],
            ];
            let sources = ["a", "b"]
                .iter()
                .zip(&data)
                .map(|(n, rows)| (n.to_string(), batch(rows)))
                .collect();
            let mut view = SqlView::new_with_optimizer(&sql, sources, optimizer())
                .await
                .unwrap();
            let mut replay = values(&view.contents().unwrap(), false);
            assert_eq!(
                replay,
                expected(kind, mode, &data[0], &data[1]),
                "bootstrap {sql}"
            );
            // Deterministic endpoint mutation sequence includes empty buckets,
            // duplicates, NULL, key moves and simultaneous first/last matches.
            let mut seed = 0x5a17_u64;
            for round in 0..36 {
                let before = view.contents().unwrap();
                let mut changes = Vec::new();
                for (side, rows) in data.iter_mut().enumerate() {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let remove = !rows.is_empty() && (round % 9 >= 5 || seed.is_multiple_of(3));
                    let (row, weight) = if remove {
                        (rows.remove((seed as usize >> 5) % rows.len()), -1)
                    } else {
                        let key = (seed >> 12) % 5;
                        let row = (
                            (key != 4).then_some(key as i64),
                            (round % 4 != 0).then_some((seed >> 20) as i64 % 3),
                        );
                        rows.push(row);
                        (row, 1)
                    };
                    changes.push(optd_incremental_sql::TableChange {
                        table: ["a", "b"][side].into(),
                        rows: batch(&[row]),
                        weights: vec![weight],
                    });
                }
                let candidate = view.prepare(&changes).await.unwrap();
                let oracle = expected(kind, mode, &data[0], &data[1]);
                assert_eq!(
                    values(candidate.contents(), false),
                    oracle,
                    "{sql} round {round}"
                );
                for (row, weight) in values(&candidate.delta().unwrap(), true) {
                    add(&mut replay, row, weight);
                }
                assert_eq!(replay, oracle, "signed prefix {sql} round {round}");
                assert_eq!(view.contents().unwrap(), before, "candidate isolation");
                let node = candidate
                    .stats()
                    .nodes
                    .iter()
                    .find(|n| n.strategy == optd_incremental_sql::Strategy::MatchCountDelta)
                    .unwrap();
                let work = node.keyed_work.as_ref().unwrap();
                assert_eq!(
                    work.snapshot_evaluations_before + work.snapshot_evaluations_after,
                    0
                );
                assert_eq!(work.full_input_scans, 0);
                assert!(work.match_join_work.is_some());
                if round % 12 == 0 {
                    let expected_full = full(&sql, &data).await;
                    assert_eq!(candidate.contents().schema(), expected_full.schema());
                    assert_eq!(values(&expected_full, false), oracle, "full SQL {sql}");
                    drop(candidate);
                    let candidate = view.prepare(&changes).await.unwrap();
                    assert_eq!(
                        values(candidate.contents(), false),
                        oracle,
                        "discard/reprepare"
                    );
                    view.acknowledge(candidate).unwrap();
                } else {
                    view.acknowledge(candidate).unwrap();
                }
            }
        }
    }
}

#[tokio::test]
async fn hot_semi_anti_no_crossing_never_visits_unchanged_peers() {
    for kind in ["LEFT SEMI", "LEFT ANTI", "RIGHT SEMI", "RIGHT ANTI"] {
        let mut observed = Vec::new();
        for size in [8_i64, 1600] {
            let a = (0..size).map(|v| (Some(1), Some(v))).collect::<Vec<_>>();
            let data = BTreeMap::from([("a".into(), batch(&a)), ("b".into(), batch(&a))]);
            let sql = format!("SELECT * FROM a {kind} JOIN b ON a.k=b.k");
            let view = SqlView::new_with_optimizer(&sql, data, optimizer())
                .await
                .unwrap();
            let candidate = view
                .prepare(&[optd_incremental_sql::TableChange {
                    // Change the preserved side so SQL column pruning retains v.
                    table: if kind.starts_with("RIGHT") { "b" } else { "a" }.into(),
                    rows: batch(&[(Some(1), Some(size))]),
                    weights: vec![1],
                }])
                .await
                .unwrap();
            let work = candidate
                .stats()
                .nodes
                .iter()
                .find_map(|n| n.keyed_work.as_ref())
                .unwrap();
            let matches = work.match_join_work.as_ref().unwrap();
            assert_eq!(matches.peer_entries_visited, 0, "{kind}");
            assert_eq!(matches.matched_pairs, 0);
            assert_eq!(matches.zero_crossings, 0);
            assert_eq!(matches.changed_entries, 1);
            assert_eq!(matches.count_probes, 1);
            assert_eq!(matches.bucket_rows_materialized, size as usize + 1);
            assert_eq!(matches.bucket_entries_copied, size as usize);
            observed.push(matches.clone());
        }
        assert_eq!(observed[0].count_probes, observed[1].count_probes);
    }
}

#[tokio::test]
async fn removing_match_rule_retains_scoped_residual_alternative_and_not_in_is_rejected() {
    let sql = "SELECT * FROM a LEFT JOIN b ON a.k=b.k AND a.v<b.v";
    let view = SqlView::new_with_optimizer(
        sql,
        tables(),
        optimizer()
            .without_rule("builtin.match_count_join")
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(view
        .incremental_plan()
        .nodes()
        .iter()
        .any(|n| n.rule.name == "builtin.keyed_join"));
    assert!(!view
        .incremental_plan()
        .nodes()
        .iter()
        .any(|n| n.rule.name == "builtin.match_count_join"));
    let error = SqlView::new_with_optimizer(
        "SELECT a.k FROM a WHERE a.k NOT IN (SELECT b.k FROM b)",
        tables(),
        optimizer(),
    )
    .await
    .err()
    .unwrap();
    assert!(!error.plan_preflight().unwrap().strict_plan_eligible);
}

#[tokio::test]
async fn first_last_matches_empty_inputs_and_stale_candidates_are_exact() {
    for kind in KINDS {
        for (mode, condition) in [
            (0, "a.k=b.k"),
            (1, "a.k IS NOT DISTINCT FROM b.k"),
            // These directed rows all have v=1, so <= retains ordinary-key matches.
            (0, "a.k=b.k AND a.v<=b.v"),
        ] {
            let sql = format!("SELECT * FROM a {kind} JOIN b ON {condition}");
            let mut view = SqlView::new_with_optimizer(
                &sql,
                BTreeMap::from([("a".into(), batch(&[])), ("b".into(), batch(&[]))]),
                optimizer(),
            )
            .await
            .unwrap();
            let mut data: [Vec<Row>; 2] = [vec![], vec![]];
            for (left, right) in [(2, 0), (2, 2), (1, 1), (1, 0), (0, 2), (2, 1), (0, 0)] {
                let next = [
                    vec![(Some(1), Some(1)); left],
                    vec![(Some(1), Some(1)); right],
                ];
                let mut changes = vec![];
                for side in 0..2 {
                    let rows = data[side]
                        .iter()
                        .chain(next[side].iter())
                        .copied()
                        .collect::<Vec<_>>();
                    let weights = std::iter::repeat_n(-1, data[side].len())
                        .chain(std::iter::repeat_n(1, next[side].len()))
                        .collect();
                    changes.push(optd_incremental_sql::TableChange {
                        table: ["a", "b"][side].into(),
                        rows: batch(&rows),
                        weights,
                    });
                }
                let prepared = view.prepare(&changes).await.unwrap();
                let stale = view.prepare(&changes).await.unwrap();
                assert_eq!(
                    values(prepared.contents(), false),
                    expected(kind, mode, &next[0], &next[1]),
                    "{sql} {left}/{right}"
                );
                view.acknowledge(prepared).unwrap();
                assert!(view.acknowledge(stale).is_err());
                data = next;
            }
            let before = view.contents().unwrap();
            assert!(view
                .prepare(&[optd_incremental_sql::TableChange {
                    table: "a".into(),
                    rows: batch(&[(Some(9), Some(9))]),
                    weights: vec![-1]
                }])
                .await
                .is_err());
            assert_eq!(view.contents().unwrap(), before);
        }
    }
}

#[tokio::test]
async fn untouched_keys_do_not_increase_match_or_bucket_work() {
    let mut observations = vec![];
    for size in [16, 2000] {
        let mut rows = (10..size).map(|k| (Some(k), Some(k))).collect::<Vec<_>>();
        rows.extend([(Some(1), Some(1)), (Some(1), Some(2))]);
        let view = SqlView::new_with_optimizer(
            "SELECT * FROM a FULL JOIN b ON a.k=b.k",
            BTreeMap::from([("a".into(), batch(&rows)), ("b".into(), batch(&rows))]),
            optimizer(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[optd_incremental_sql::TableChange {
                table: "a".into(),
                rows: batch(&[(Some(1), Some(3))]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.keyed_work.as_ref())
            .unwrap();
        assert_eq!(work.touched_keys, 1);
        assert_eq!(work.snapshot_input_rows_before, vec![0, 0]);
        assert_eq!(work.snapshot_input_rows_after, vec![0, 0]);
        let matches = work.match_join_work.as_ref().unwrap();
        assert_eq!(matches.peer_entries_visited, 2);
        assert_eq!(matches.matched_pairs, 2);
        observations.push(matches.clone());
    }
    assert_eq!(observations[0], observations[1]);
}

#[tokio::test]
async fn computed_keys_using_self_join_and_downstream_operators_recover() {
    for sql in [
        "SELECT * FROM a FULL JOIN b ON a.k+1=b.k",
        "SELECT * FROM a FULL JOIN b USING(k)",
        "SELECT * FROM a FULL JOIN a AS b ON a.k=b.k",
        "SELECT * FROM a FULL JOIN a AS b ON a.k=b.k AND a.v<b.v",
        "SELECT * FROM a FULL JOIN b ON (a.k IS NOT DISTINCT FROM b.k) AND a.v<b.v",
        "SELECT a.k, count(b.v) AS n FROM a LEFT JOIN b ON a.k=b.k AND a.v<b.v GROUP BY a.k",
        "SELECT a.k, count(b.v) AS n FROM a LEFT JOIN b ON a.k=b.k GROUP BY a.k",
        "SELECT a.k,b.v,row_number() OVER (PARTITION BY a.k ORDER BY b.v) AS rn FROM a LEFT JOIN b ON a.k=b.k",
    ] {
        let root = tempfile::tempdir().unwrap();
        let data = [vec![(Some(1),Some(10)),(Some(1),Some(10)),(None,None)],vec![(Some(2),Some(20)),(None,None)]];
        let sources = ["a","b"].iter().zip(&data).map(|(n,rows)|(n.to_string(),batch(rows))).collect();
        let mut durable = optd_incremental_sql::DurableSqlView::open_with_optimizer(root.path(),sql,sources,optimizer()).await.unwrap();
        assert!(durable.view().incremental_plan().nodes().iter().any(|n| n.rule.name == "builtin.match_count_join"),"{sql}");
        for (round, value) in [Some(1),None,Some(2)].iter().enumerate() {
            durable.refresh(&[optd_incremental_sql::TableChange { table:"a".into(),rows:batch(&[(*value,Some(round as i64))]),weights:vec![1] }]).await.unwrap();
            let sources = durable.view().tables().unwrap();
            let full_data = [sources["a"].clone(),sources["b"].clone()];
            let ctx = datafusion::prelude::SessionContext::new();
            for (name, rows) in ["a","b"].iter().zip(full_data) { ctx.register_batch(*name,rows).unwrap(); }
            let frame = ctx.sql(sql).await.unwrap();
            let full_schema = Arc::new(frame.schema().as_arrow().clone());
            let result = datafusion::arrow::compute::concat_batches(&full_schema,&frame.collect().await.unwrap()).unwrap();
            let contents = durable.view().contents().unwrap();
            assert_eq!(contents.schema(),result.schema());
            assert_eq!(scalar_rows(&contents),scalar_rows(&result),"{sql}");
            drop(durable);
            durable = optd_incremental_sql::DurableSqlView::open_with_optimizer(root.path(),sql,BTreeMap::new(),optimizer()).await.unwrap();
            assert_eq!(durable.view().epoch(),round as u64 + 1);
            assert_eq!(scalar_rows(&durable.view().contents().unwrap()),scalar_rows(&result));
        }
    }
}

fn scalar_rows(
    batch: &RecordBatch,
) -> std::collections::HashMap<Vec<datafusion::common::ScalarValue>, i64> {
    let mut rows = std::collections::HashMap::new();
    for i in 0..batch.num_rows() {
        let row = batch
            .columns()
            .iter()
            .map(|c| datafusion::common::ScalarValue::try_from_array(c, i).unwrap())
            .collect();
        *rows.entry(row).or_default() += 1;
    }
    rows
}

#[tokio::test]
async fn physical_verifier_rejects_forged_match_operator_state_and_token() {
    use optd_core::heuristics::HeuristicsOptimizer;
    use optd_core::nodes::PlanNodeOrGroup;
    use optd_core::rules::{Rule, RuleMatcher};
    use optd_incremental_sql::planner::plan::{
        logical, IncrementalNode, IncrementalRel, Operation,
    };
    struct Corrupt {
        matcher: RuleMatcher<IncrementalNode>,
        mode: usize,
    }
    impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for Corrupt {
        fn name(&self) -> &'static str {
            "test.corrupt_match_join"
        }
        fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
            &self.matcher
        }
        fn apply(
            &self,
            _: &HeuristicsOptimizer<IncrementalNode>,
            binding: IncrementalRel,
        ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
            let IncrementalNode::Logical(Operation::MaintainMatchJoin { state, operator }) =
                &binding.typ
            else {
                return vec![];
            };
            let mut operator = operator.clone();
            let mut state = *state;
            let mut children = binding
                .children
                .iter()
                .map(|child| {
                    let PlanNodeOrGroup::PlanNode(child) = child else {
                        panic!("compiled tree has no memo groups")
                    };
                    child.clone()
                })
                .collect::<Vec<_>>();
            match self.mode {
                0 => state += 1,
                1 => {
                    let datafusion::logical_expr::LogicalPlan::Join(join) =
                        Arc::make_mut(&mut operator.template)
                    else {
                        panic!("typed join")
                    };
                    join.null_equality = datafusion::common::NullEquality::NullEqualsNull;
                }
                _ => children.clear(),
            }
            vec![logical(Operation::MaintainMatchJoin { state, operator }, children).into()]
        }
    }
    for mode in 0..3 {
        let rule = Corrupt {
            mode,
            matcher: RuleMatcher::MatchDiscriminant {
                typ_discriminant: std::mem::discriminant(&IncrementalNode::Logical(Operation::Sum)),
                children: vec![RuleMatcher::AnyMany],
            },
        };
        let error = SqlView::new_with_optimizer(
            "SELECT * FROM a FULL JOIN b ON a.k=b.k",
            tables(),
            optimizer().with_plan_rule(Arc::new(rule)),
        )
        .await
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("incremental Optd plan"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn residual_outer_semi_anti_plans_use_match_counts_when_enabled() {
    for kind in KINDS {
        let sql = format!("SELECT * FROM a {kind} JOIN b ON a.k=b.k AND a.v<b.v");
        let view = SqlView::new_with_optimizer(&sql, tables(), optimizer())
            .await
            .unwrap();
        assert!(
            view.incremental_plan()
                .nodes()
                .iter()
                .any(|n| n.rule.name == "builtin.match_count_join"),
            "{sql}: {}",
            view.incremental_plan().explain()
        );
        assert!(!view
            .incremental_plan()
            .programs()
            .iter()
            .flat_map(|p| &p.steps)
            .any(|s| s.operation.name() == "EvaluateSnapshot"));
    }
}

#[tokio::test]
async fn residual_hot_bucket_replacement_consolidates_counts_before_zero_crossings() {
    for kind in ["LEFT SEMI", "LEFT ANTI", "RIGHT SEMI", "RIGHT ANTI"] {
        for size in [8_i64, 1400] {
            let preserved = (0..size).map(|v| (Some(1), Some(v))).collect::<Vec<_>>();
            let right_preserved = kind.starts_with("RIGHT");
            let old = if right_preserved { -2 } else { size + 1 };
            let new = if right_preserved { -1 } else { size + 17 };
            // Fifteen other rows in the changed bucket remain untouched. Their
            // peer combinations must not be evaluated again during refresh.
            let peer = (0..16)
                .map(|v| {
                    (
                        Some(1),
                        Some(if right_preserved {
                            -2 - v
                        } else {
                            size + 1 + v
                        }),
                    )
                })
                .collect::<Vec<_>>();
            let data = if right_preserved {
                [peer, preserved]
            } else {
                [preserved, peer]
            };
            let sources = ["a", "b"]
                .iter()
                .zip(&data)
                .map(|(n, rows)| (n.to_string(), batch(rows)))
                .collect();
            let sql = format!("SELECT * FROM a {kind} JOIN b ON a.k=b.k AND a.v<b.v");
            let view = SqlView::new_with_optimizer(&sql, sources, optimizer())
                .await
                .unwrap();
            let candidate = view
                .prepare(&[optd_incremental_sql::TableChange {
                    table: if right_preserved { "a" } else { "b" }.into(),
                    rows: batch(&[(Some(1), Some(old)), (Some(1), Some(new))]),
                    weights: vec![-1, 1],
                }])
                .await
                .unwrap();
            assert_eq!(candidate.contents(), &view.contents().unwrap());
            assert_eq!(candidate.delta().unwrap().num_rows(), 0);
            let work = candidate
                .stats()
                .nodes
                .iter()
                .find_map(|n| n.keyed_work.as_ref())
                .unwrap()
                .match_join_work
                .as_ref()
                .unwrap();
            assert_eq!(work.changed_entries, 2);
            assert_eq!(work.residual_candidate_pairs, 2 * size as usize);
            assert_eq!(work.residual_predicate_rows, 2 * size as usize);
            assert_eq!(work.matched_pairs, 0);
            assert_eq!(work.zero_crossings, 0);
            assert_eq!(work.match_count_updates, 2);
            assert_eq!(work.count_map_entries_copied, size as usize + 16);
        }
    }
}

#[tokio::test]
async fn residual_predicate_failure_does_not_install_input_or_counts() {
    let sql = "SELECT * FROM a FULL JOIN b ON a.k=b.k AND 10/(a.v-b.v)>0";
    let data = [vec![(Some(1), Some(10))], vec![(Some(1), Some(1))]];
    let sources = ["a", "b"]
        .iter()
        .zip(&data)
        .map(|(n, rows)| (n.to_string(), batch(rows)))
        .collect();
    let mut view = SqlView::new_with_optimizer(sql, sources, optimizer())
        .await
        .unwrap();
    let before = view.contents().unwrap();
    let before_tables = view.tables().unwrap();
    let bad = optd_incremental_sql::TableChange {
        table: "b".into(),
        rows: batch(&[(Some(1), Some(10))]),
        weights: vec![1],
    };
    let left_stage = optd_incremental_sql::TableChange {
        table: "a".into(),
        rows: batch(&[(Some(1), Some(11))]),
        weights: vec![1],
    };
    // Stage 0 succeeds privately; the new right row fails in stage 1.
    assert!(view.prepare(&[left_stage, bad]).await.is_err());
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.epoch(), 0);
    assert_eq!(view.tables().unwrap(), before_tables);
    let good = optd_incremental_sql::TableChange {
        table: "b".into(),
        // Retract the existing peer whose count stage 0 had changed. A leaked
        // count would now disagree with the fixed committed left input.
        rows: batch(&[(Some(1), Some(1)), (Some(1), Some(2))]),
        weights: vec![-1, 1],
    };
    let candidate = view.prepare(&[good]).await.unwrap();
    let expected = full(sql, &[data[0].clone(), vec![(Some(1), Some(2))]]).await;
    assert_eq!(scalar_rows(candidate.contents()), scalar_rows(&expected));
    view.acknowledge(candidate).unwrap();
}

#[tokio::test]
async fn residual_work_grows_with_changed_pairs_not_untouched_keys() {
    let mut observations = vec![];
    for size in [16_i64, 2000] {
        let rows = (10..size)
            .map(|k| (Some(k), Some(k)))
            .chain([(Some(1), Some(1)), (Some(1), Some(2))])
            .collect::<Vec<_>>();
        let view = SqlView::new_with_optimizer(
            "SELECT * FROM a FULL JOIN b ON a.k=b.k AND a.v<b.v",
            BTreeMap::from([("a".into(), batch(&rows)), ("b".into(), batch(&rows))]),
            optimizer(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[optd_incremental_sql::TableChange {
                table: "a".into(),
                rows: batch(&[(Some(1), Some(3))]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        let work = candidate
            .stats()
            .nodes
            .iter()
            .find_map(|n| n.keyed_work.as_ref())
            .unwrap();
        assert_eq!(work.touched_keys, 1);
        assert_eq!(work.full_input_scans, 0);
        assert_eq!(
            work.snapshot_evaluations_before + work.snapshot_evaluations_after,
            0
        );
        let mut matches = work.match_join_work.clone().unwrap();
        assert_eq!(matches.residual_candidate_pairs, 2);
        assert_eq!(matches.residual_predicate_rows, 2);
        assert_eq!(matches.count_map_entries_copied, 4);
        assert_eq!(matches.match_count_updates, 1);
        // Persistent directory path copies may grow logarithmically with key count.
        assert!(
            matches.count_directory_entries_copied <= 64 * (size as usize + 1).ilog2() as usize
        );
        matches.count_directory_entries_copied = 0;
        observations.push(matches);
    }
    assert_eq!(observations[0], observations[1]);
}

#[tokio::test]
async fn final_program_reports_scoped_work_added_by_an_explicit_optd_rule() {
    use optd_core::heuristics::HeuristicsOptimizer;
    use optd_core::nodes::PlanNodeOrGroup;
    use optd_core::rules::{Rule, RuleMatcher};
    use optd_incremental_sql::planner::plan::{
        logical, IncrementalNode, IncrementalRel, Operation,
    };
    use optd_incremental_sql::planner::SnapshotVersion;
    struct SnapshotProbe {
        matcher: RuleMatcher<IncrementalNode>,
    }
    impl Rule<IncrementalNode, HeuristicsOptimizer<IncrementalNode>> for SnapshotProbe {
        fn name(&self) -> &'static str {
            "test.probe_match_snapshot"
        }
        fn matcher(&self) -> &RuleMatcher<IncrementalNode> {
            &self.matcher
        }
        fn apply(
            &self,
            _: &HeuristicsOptimizer<IncrementalNode>,
            binding: IncrementalRel,
        ) -> Vec<PlanNodeOrGroup<IncrementalNode>> {
            let IncrementalNode::Logical(Operation::MaintainMatchJoin { state, operator }) =
                &binding.typ
            else {
                return vec![];
            };
            let PlanNodeOrGroup::PlanNode(token) = &binding.children[0] else {
                panic!("compiled token")
            };
            let inputs = operator
                .inputs
                .iter()
                .enumerate()
                .map(|(port, schema)| {
                    logical(
                        Operation::IndexLookup {
                            state: *state,
                            port,
                            version: SnapshotVersion::After,
                            schema: Arc::new(schema.as_arrow().clone()),
                        },
                        vec![token.clone()],
                    )
                })
                .collect();
            let snapshot = logical(
                Operation::EvaluateSnapshot {
                    state: *state,
                    version: SnapshotVersion::After,
                    operator: operator.clone(),
                },
                inputs,
            );
            // Adding S-S preserves the signed law but intentionally executes one
            // shared scoped evaluation, whose work must stay visible.
            vec![logical(
                Operation::Sum,
                vec![
                    binding.clone(),
                    snapshot.clone(),
                    logical(Operation::Negate, vec![snapshot]),
                ],
            )
            .into()]
        }
    }
    let rule = SnapshotProbe {
        matcher: RuleMatcher::MatchDiscriminant {
            typ_discriminant: std::mem::discriminant(&IncrementalNode::Logical(Operation::Sum)),
            children: vec![RuleMatcher::AnyMany],
        },
    };
    let sql = "SELECT * FROM a FULL JOIN b ON a.k=b.k AND a.v<b.v";
    let view =
        SqlView::new_with_optimizer(sql, tables(), optimizer().with_plan_rule(Arc::new(rule)))
            .await
            .unwrap();
    let candidate = view
        .prepare(&[optd_incremental_sql::TableChange {
            table: "a".into(),
            rows: batch(&[(Some(1), Some(20))]),
            weights: vec![1],
        }])
        .await
        .unwrap();
    let work = candidate
        .stats()
        .nodes
        .iter()
        .find(|n| n.operator.contains(" Join:"))
        .unwrap();
    assert_eq!(
        work.strategy,
        optd_incremental_sql::Strategy::KeyedRecompute
    );
    let keyed = work.keyed_work.as_ref().unwrap();
    assert_eq!(keyed.snapshot_evaluations_after, 1);
    assert!(keyed.match_join_work.is_some());
    let data = [
        vec![
            (Some(1), Some(10)),
            (Some(1), Some(10)),
            (Some(2), None),
            (None, Some(30)),
            (Some(1), Some(20)),
        ],
        vec![(Some(1), Some(40)), (Some(3), Some(50)), (None, Some(60))],
    ];
    let reference = full(sql, &data).await;
    assert_eq!(scalar_rows(candidate.contents()), scalar_rows(&reference));
}
