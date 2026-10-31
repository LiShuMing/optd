//! Boolean mark rules retain SQL membership and replace flags at zero crossings.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy};

type Row = (Option<i64>, Option<i64>);

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

fn tables(data: &[Vec<Row>; 2]) -> BTreeMap<String, RecordBatch> {
    ["a", "b"]
        .iter()
        .zip(data)
        .map(|(name, rows)| (name.to_string(), batch(rows)))
        .collect()
}

const QUERIES: [&str; 4] = [
    "SELECT a.k,a.v FROM a WHERE a.v=9 OR EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
    "SELECT a.k,a.v FROM a WHERE a.v=9 OR NOT EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
    "SELECT a.k,a.v FROM a WHERE a.v=9 OR a.k IN (SELECT b.k FROM b)",
    "SELECT a.k,a.v FROM a WHERE a.v=9 OR EXISTS (SELECT 1 FROM b WHERE b.k=a.k AND a.v<b.v)",
];

#[tokio::test]
async fn boolean_mark_disjunctions_select_separate_explicit_count_rule() {
    let data = [
        vec![(Some(1), Some(1)), (None, None)],
        vec![(Some(1), Some(2))],
    ];
    for query in QUERIES {
        let view = SqlView::new_with_optimizer(
            query,
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let plan = view.incremental_plan();
        let marks = plan
            .nodes()
            .iter()
            .zip(plan.programs())
            .filter(|(n, _)| n.operator.contains("Mark Join:"))
            .collect::<Vec<_>>();
        assert_eq!(marks.len(), 1, "{query}");
        for (node, program) in marks {
            assert_eq!(node.rule.name, "builtin.boolean_mark_join");
            assert_eq!(node.rule.version, 1);
            assert_eq!(program.strategy(), Strategy::MatchCountDelta);
            assert!(program
                .steps
                .iter()
                .any(|s| s.operation.name() == "MaintainMatchJoin"));
            assert!(!program
                .steps
                .iter()
                .any(|s| s.operation.name() == "EvaluateSnapshot"));
        }
    }
}

type Rows = BTreeMap<Vec<String>, i64>;

fn add(rows: &mut Rows, row: Vec<String>, weight: i64) {
    let next = rows.get(&row).copied().unwrap_or_default() + weight;
    if next == 0 {
        rows.remove(&row);
    } else {
        rows.insert(row, next);
    }
}

fn values(batch: &RecordBatch, signed: bool) -> Rows {
    let mut result = Rows::new();
    for mut row in optd_incremental_sql::database::display_rows(batch).unwrap() {
        let weight = if signed {
            row.pop().unwrap().parse().unwrap()
        } else {
            1
        };
        add(&mut result, row, weight);
    }
    result
}

/// Integer/NULL nested-loop truth; positive IN observes TRUE rows only.
fn expected(mode: usize, data: &[Vec<Row>; 2]) -> Rows {
    let mut result = Rows::new();
    for a in &data[0] {
        let matched = data[1].iter().any(|b| {
            a.0.is_some()
                && a.0 == b.0
                && (mode != 3 || matches!((a.1,b.1),(Some(a),Some(b)) if a<b))
        });
        if a.1 == Some(9) || (if mode == 1 { !matched } else { matched }) {
            add(
                &mut result,
                vec![
                    a.0.map_or("NULL".into(), |v| v.to_string()),
                    a.1.map_or("NULL".into(), |v| v.to_string()),
                ],
                1,
            );
        }
    }
    result
}

async fn full(query: &str, data: &[Vec<Row>; 2]) -> RecordBatch {
    let ctx = datafusion::prelude::SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_target_partitions(1),
    );
    for (name, rows) in tables(data) {
        ctx.register_batch(&name, rows).unwrap();
    }
    let frame = ctx.sql(query).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    datafusion::arrow::compute::concat_batches(&schema, &frame.collect().await.unwrap()).unwrap()
}

#[tokio::test]
async fn signed_two_sided_marks_match_independent_truth_and_full_sql_prefixes() {
    for (mode, query) in QUERIES.iter().enumerate() {
        let mut data = [
            vec![
                (Some(1), Some(1)),
                (Some(1), Some(1)),
                (Some(2), Some(9)),
                (None, None),
            ],
            vec![(Some(1), Some(2)), (None, None)],
        ];
        let mut view = SqlView::new_with_optimizer(
            query,
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let mut replay = values(&view.contents().unwrap(), false);
        assert_eq!(replay, expected(mode, &data));
        let mut seed = 0x87e2_u64;
        for round in 0..48 {
            let committed = view.contents().unwrap();
            let mut changes = Vec::new();
            for (side, rows) in data.iter_mut().enumerate() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let (row, weight) =
                    if !rows.is_empty() && (round % 12 > 6 || seed.is_multiple_of(3)) {
                        (rows.remove((seed as usize >> 6) % rows.len()), -1)
                    } else {
                        let key = (seed >> 10) % 5;
                        let row = (
                            (key != 4).then_some(key as i64),
                            (round % 4 != 0).then_some(if round % 5 == 0 {
                                9
                            } else {
                                (seed >> 20) as i64 % 4
                            }),
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
            let discarded = view.prepare(&changes).await.unwrap();
            assert_eq!(values(discarded.contents(), false), expected(mode, &data));
            drop(discarded);
            assert_eq!(view.contents().unwrap(), committed);
            let candidate = view.prepare(&changes).await.unwrap();
            for (row, weight) in values(&candidate.delta().unwrap(), true) {
                add(&mut replay, row, weight);
            }
            assert_eq!(replay, expected(mode, &data), "mode {mode} round {round}");
            for node in candidate
                .stats()
                .nodes
                .iter()
                .filter(|n| n.operator.contains("Mark Join:"))
            {
                assert_eq!(node.strategy, Strategy::MatchCountDelta);
                let work = node.keyed_work.as_ref().unwrap();
                assert_eq!(
                    work.snapshot_evaluations_before + work.snapshot_evaluations_after,
                    0
                );
                assert_eq!(work.full_input_scans, 0);
                assert!(work.match_join_work.is_some());
            }
            if round % 12 == 0 {
                let reference = full(query, &data).await;
                assert_eq!(candidate.contents().schema(), reference.schema());
                assert_eq!(values(&reference, false), replay);
            }
            let stale = view.prepare(&changes).await.unwrap();
            view.acknowledge(candidate).unwrap();
            assert!(view.acknowledge(stale).is_err());
        }
    }
}

#[tokio::test]
async fn unchanged_pure_mark_positivity_visits_no_preserved_peer_rows() {
    for untouched in [8, 2048] {
        let mut data = [
            (0..32).map(|i| (Some(1), Some(i))).collect(),
            vec![(Some(1), Some(1))],
        ];
        for key in 2..untouched {
            data[0].push((Some(key), Some(0)));
            data[1].push((Some(key), Some(1)));
        }
        let view = SqlView::new_with_optimizer(
            QUERIES[0],
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let candidate = view
            .prepare(&[optd_incremental_sql::TableChange {
                table: "b".into(),
                rows: batch(&[(Some(1), Some(2))]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        assert_eq!(candidate.contents(), &view.contents().unwrap());
        let node = candidate
            .stats()
            .nodes
            .iter()
            .find(|n| n.operator.contains("Mark Join:"))
            .unwrap();
        let work = node
            .keyed_work
            .as_ref()
            .unwrap()
            .match_join_work
            .as_ref()
            .unwrap();
        assert_eq!(work.peer_entries_visited, 0);
        assert_eq!(work.zero_crossings, 0);
        assert_eq!(work.emitted_contributions, 0);
        assert_eq!(work.changed_entries, 1);
        assert_eq!(work.bucket_entries_copied, 1);
    }
}

#[tokio::test]
async fn removing_mark_count_rule_keeps_an_explicit_scoped_alternative() {
    let data = [
        vec![(Some(1), Some(1)), (None, None)],
        vec![(Some(1), Some(2))],
    ];
    let optimizer = IncrementalOptimizer::default()
        .without_rule("builtin.boolean_mark_join")
        .unwrap()
        .require_incremental();
    let view = SqlView::new_with_optimizer(QUERIES[0], tables(&data), optimizer)
        .await
        .unwrap();
    let mark = view
        .incremental_plan()
        .nodes()
        .iter()
        .find(|n| n.operator.contains("Mark Join:"))
        .unwrap();
    assert_eq!(mark.rule.name, "builtin.keyed_join");
    let candidate = view
        .prepare(&[optd_incremental_sql::TableChange {
            table: "b".into(),
            rows: batch(&data[1]),
            weights: vec![-1],
        }])
        .await
        .unwrap();
    let reference = expected(0, &[data[0].clone(), vec![]]);
    assert_eq!(values(candidate.contents(), false), reference);
    let node = candidate
        .stats()
        .nodes
        .iter()
        .find(|n| n.operator.contains("Mark Join:"))
        .unwrap();
    assert_eq!(node.strategy, Strategy::KeyedRecompute);
    let work = node.keyed_work.as_ref().unwrap();
    assert!(work.snapshot_evaluations_before + work.snapshot_evaluations_after > 0);
}

#[tokio::test]
async fn residual_mark_failure_discards_prior_stage_counts() {
    let query="SELECT a.k,a.v FROM a WHERE a.v=9 OR EXISTS (SELECT 1 FROM b WHERE a.k=b.k AND 10/(a.v-b.v)>0)";
    let data = [vec![(Some(1), Some(10))], vec![(Some(1), Some(1))]];
    let mut view = SqlView::new_with_optimizer(
        query,
        tables(&data),
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    let before = view.contents().unwrap();
    let sources = view.tables().unwrap();
    assert!(view
        .prepare(&[
            optd_incremental_sql::TableChange {
                table: "a".into(),
                rows: batch(&[(Some(1), Some(11))]),
                weights: vec![1]
            },
            optd_incremental_sql::TableChange {
                table: "b".into(),
                rows: batch(&[(Some(1), Some(10))]),
                weights: vec![1]
            },
        ])
        .await
        .is_err());
    assert_eq!(view.contents().unwrap(), before);
    assert_eq!(view.tables().unwrap(), sources);
    assert_eq!(view.epoch(), 0);
    let candidate = view
        .prepare(&[optd_incremental_sql::TableChange {
            table: "b".into(),
            rows: batch(&[(Some(1), Some(1)), (Some(1), Some(2))]),
            weights: vec![-1, 1],
        }])
        .await
        .unwrap();
    let reference = full(query, &[data[0].clone(), vec![(Some(1), Some(2))]]).await;
    assert_eq!(
        values(candidate.contents(), false),
        values(&reference, false)
    );
    view.acknowledge(candidate).unwrap();
}

/// SQL writes have an independent endpoint model, never read back from targets.
#[tokio::test]
async fn sql_mark_dml_retries_reopen_history_and_downstream_consumers_are_exact() {
    use optd_incremental_sql::database::SqlSession;
    for (mode, query) in QUERIES.iter().enumerate() {
        let root = tempfile::tempdir().unwrap();
        let mut db = SqlSession::open(root.path()).await.unwrap();
        db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT);CREATE TABLE b(k BIGINT,v BIGINT);INSERT INTO a VALUES (1,1),(1,1),(2,9),(NULL,NULL);INSERT INTO b VALUES (1,2),(NULL,NULL)").await.unwrap();
        let mut data = [
            vec![
                (Some(1), Some(1)),
                (Some(1), Some(1)),
                (Some(2), Some(9)),
                (None, None),
            ],
            vec![(Some(1), Some(2)), (None, None)],
        ];
        let queries = [
            ("j", query.to_string()),
            (
                "g",
                format!("SELECT k,count(*) AS n FROM ({query}) q GROUP BY k"),
            ),
            (
                "w",
                format!("SELECT q.*,count(*) OVER (PARTITION BY k) AS n FROM ({query}) q"),
            ),
        ];
        for (name, sql) in &queries {
            db.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {sql}"))
                .await
                .unwrap();
            assert!(db
                .explain_view(name)
                .unwrap()
                .contains("PhysicalMaintainMatchJoin"));
        }
        for round in 0..8 {
            let sql = match round {
                0 => {
                    data[1].retain(|r| r.0 != Some(1));
                    "DELETE FROM b WHERE k=1"
                }
                1 => {
                    data[1].extend([(Some(1), Some(2)), (Some(1), Some(2))]);
                    "INSERT INTO b VALUES (1,2),(1,2)"
                }
                2 => {
                    for row in &mut data[0] {
                        if row.0 == Some(1) {
                            row.0 = Some(3);
                            row.1 = Some(3);
                        }
                    }
                    "UPDATE a SET k=3,v=3 WHERE k=1"
                }
                3 => {
                    data[0].push((Some(1), Some(0)));
                    for row in &mut data[1] {
                        if row.0 == Some(1) {
                            row.0 = Some(3);
                            row.1 = Some(4);
                        }
                    }
                    "INSERT INTO a VALUES (1,0);UPDATE b SET k=3,v=4 WHERE k=1"
                }
                4 => {
                    data[1].clear();
                    "DELETE FROM b"
                }
                5 => {
                    data[0].clear();
                    data[1].extend([(Some(3), Some(4)), (None, None)]);
                    "DELETE FROM a;INSERT INTO b VALUES (3,4),(NULL,NULL)"
                }
                6 => {
                    data[0].extend([
                        (Some(3), Some(3)),
                        (Some(3), Some(3)),
                        (None, None),
                        (Some(2), Some(9)),
                    ]);
                    "INSERT INTO a VALUES (3,3),(3,3),(NULL,NULL),(2,9)"
                }
                _ => {
                    data[0].clear();
                    data[1].clear();
                    "DELETE FROM a;DELETE FROM b"
                }
            };
            let id = format!("mark-{round}");
            let response = if sql.contains(';') {
                db.execute_transaction(&id, sql).await
            } else {
                db.execute_request(&id, sql).await
            }
            .unwrap();
            assert!(!response.replayed);
            let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
            for (name, query) in &queries {
                let actual = db
                    .execute(&format!("SELECT * FROM {name}"))
                    .await
                    .unwrap()
                    .rows
                    .unwrap();
                let reference = full(query, &data).await;
                assert_eq!(actual.schema(), reference.schema());
                assert_eq!(
                    values(&actual, false),
                    values(&reference, false),
                    "mode {mode} round {round}"
                );
                if *name == "j" {
                    assert_eq!(values(&actual, false), expected(mode, &data));
                }
                for node in receipt["maintenance"][name]["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|n| {
                        n["operator"]
                            .as_str()
                            .is_some_and(|s| s.contains("Mark Join:"))
                    })
                {
                    assert_eq!(node["strategy"], "MatchCountDelta");
                    assert_eq!(node["keyed_work"]["snapshot_evaluations_before"], 0);
                    assert_eq!(node["keyed_work"]["snapshot_evaluations_after"], 0);
                    assert_eq!(node["keyed_work"]["full_input_scans"], 0);
                }
            }
            db.verify_output_history().unwrap();
            drop(db);
            db = SqlSession::open(root.path()).await.unwrap();
            let retry = if sql.contains(';') {
                db.execute_transaction(&id, sql).await
            } else {
                db.execute_request(&id, sql).await
            }
            .unwrap();
            assert!(retry.replayed);
            assert_eq!(
                serde_json::to_value(retry.commit.unwrap()).unwrap(),
                receipt
            );
            let reopened = db.execute("SELECT * FROM j").await.unwrap().rows.unwrap();
            assert_eq!(values(&reopened, false), expected(mode, &data));
            db.verify_output_history().unwrap();
        }
    }
}
