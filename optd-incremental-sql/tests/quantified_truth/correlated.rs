//! Equality correlation preserves each partition's independent three-valued truth.

use super::{add, membership, text, values, Rows};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, Strategy, TableChange};
use std::collections::BTreeMap;
use std::sync::Arc;

type Row = (Option<i64>, Option<i64>, Option<i64>);

fn batch(rows: &[Row]) -> RecordBatch {
    let fields = ["p", "q", "v"]
        .into_iter()
        .map(|name| Field::new(name, DataType::Int64, true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn tables(data: &[Vec<Row>; 2]) -> BTreeMap<String, RecordBatch> {
    ["a", "b"]
        .into_iter()
        .zip(data)
        .map(|(name, rows)| (name.to_string(), batch(rows)))
        .collect()
}

fn query(mode: usize) -> String {
    let condition = match mode {
        0 => "b.p=a.p",
        1 | 3 => "b.p=a.p AND b.q=a.q",
        2 => "a.p=b.p AND a.q=b.q AND (b.v>0 OR b.v IS NULL)",
        _ => unreachable!("query mode"),
    };
    if mode == 3 {
        format!("SELECT a.p,a.q,a.v FROM a WHERE a.v=9 OR a.v NOT IN (SELECT b.v FROM b WHERE {condition})")
    } else {
        format!("SELECT a.p,a.q,a.v,a.v IN (SELECT b.v FROM b WHERE {condition}) AS m,a.v NOT IN (SELECT b.v FROM b WHERE {condition}) AS n FROM a")
    }
}

fn expected(mode: usize, data: &[Vec<Row>; 2]) -> Rows {
    let mut result = Rows::new();
    for a in &data[0] {
        let peers = data[1]
            .iter()
            .filter(|b| {
                a.0.is_some()
                    && a.0 == b.0
                    && (mode == 0 || (a.1.is_some() && a.1 == b.1))
                    && (mode != 2 || b.2.is_none_or(|v| v > 0))
            })
            .map(|b| b.2)
            .collect::<Vec<_>>();
        let truth = membership(a.2, &peers);
        let mut row = vec![text(a.0), text(a.1), text(a.2)];
        if mode == 3 {
            if a.2 != Some(9) && truth != Some(false) {
                continue;
            }
        } else {
            row.push(truth.map_or("NULL".into(), |v| v.to_string()));
            row.push(truth.map(|v| !v).map_or("NULL".into(), |v| v.to_string()));
        }
        add(&mut result, row, 1);
    }
    result
}

#[tokio::test]
async fn correlated_projected_membership_preserves_partition_unknown() {
    let data = [
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(1), Some(2)),
            (Some(1), Some(1), None),
            (Some(2), Some(2), None),
            (None, Some(1), None),
            (Some(1), None, None),
        ],
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(1), None),
            (None, Some(1), None),
            (Some(1), None, None),
        ],
    ];
    for mode in 0..4 {
        let view = SqlView::new_with_optimizer(
            &query(mode),
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        assert_eq!(
            values(&view.contents().unwrap(), false),
            expected(mode, &data)
        );
        assert!(view
            .incremental_plan()
            .programs()
            .iter()
            .all(|p| p.strategy() != Strategy::OperatorRecompute));
    }
}

#[tokio::test]
async fn correlated_two_source_signed_prefixes_cover_partition_key_moves() {
    for mode in 0..4 {
        let mut data = [
            vec![
                (Some(1), Some(1), Some(1)),
                (Some(1), Some(1), Some(2)),
                (Some(1), Some(1), Some(2)),
                (Some(2), Some(2), None),
                (None, Some(1), None),
            ],
            vec![(Some(1), Some(1), Some(1)), (Some(2), Some(2), Some(2))],
        ];
        let mut view = SqlView::new_with_optimizer(
            &query(mode),
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let mut replay = values(&view.contents().unwrap(), false);
        let mut seed = 0x9b6_u64;
        let directed = [
            ((Some(1), Some(1), None), 1),
            ((Some(1), Some(1), None), 1),
            ((Some(1), Some(1), None), -1),
            ((Some(1), Some(1), None), -1),
            ((Some(1), Some(1), Some(1)), -1),
            ((Some(1), Some(1), Some(3)), 1),
        ];
        for round in 0..32 {
            let before = view.contents().unwrap();
            let mut changes = Vec::new();
            for (side, rows) in data.iter_mut().enumerate() {
                if side == 0 && round < directed.len() {
                    continue;
                }
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let (row, weight) = if let Some(change) = directed.get(round).filter(|_| side == 1)
                {
                    *change
                } else if !rows.is_empty() && seed.is_multiple_of(3) {
                    (rows[(seed as usize >> 12) % rows.len()], -1)
                } else {
                    let key = (seed >> 20) % 4;
                    let subkey = (seed >> 24) % 3;
                    let value = (seed >> 30) % 5;
                    (
                        (
                            (key != 3).then_some(key as i64),
                            (subkey != 2).then_some(subkey as i64),
                            (value != 4).then_some(value as i64),
                        ),
                        1,
                    )
                };
                if weight == 1 {
                    rows.push(row);
                } else {
                    let index = rows.iter().position(|v| *v == row).unwrap();
                    rows.remove(index);
                }
                changes.push(TableChange {
                    table: ["a", "b"][side].into(),
                    rows: batch(&[row]),
                    weights: vec![weight],
                });
            }
            let reference = expected(mode, &data);
            let discarded = view.prepare(&changes).await.unwrap();
            assert_eq!(values(discarded.contents(), false), reference);
            drop(discarded);
            assert_eq!(view.contents().unwrap(), before);
            let candidate = view.prepare(&changes).await.unwrap();
            for (row, weight) in values(&candidate.delta().unwrap(), true) {
                add(&mut replay, row, weight);
            }
            assert_eq!(replay, reference, "mode {mode} round {round}");
            assert_eq!(values(candidate.contents(), false), reference);
            assert!(candidate
                .stats()
                .nodes
                .iter()
                .all(|n| n.strategy != Strategy::OperatorRecompute));
            let stale = view.prepare(&changes).await.unwrap();
            view.acknowledge(candidate).unwrap();
            assert!(view.acknowledge(stale).is_err());
        }
    }
}

#[tokio::test]
async fn partition_null_crossing_never_visits_untouched_partitions() {
    let mut observed = Vec::new();
    for untouched in [8, 512] {
        let mut data = [
            vec![(Some(1), Some(1), Some(2)), (Some(1), Some(1), None)],
            vec![(Some(1), Some(1), Some(1))],
        ];
        for partition in 2..untouched {
            data[0].push((Some(partition), Some(1), Some(2)));
            data[1].push((Some(partition), Some(1), Some(1)));
        }
        let view = SqlView::new_with_optimizer(
            &query(1),
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let row = (Some(1), Some(1), None);
        let candidate = view
            .prepare(&[TableChange {
                table: "b".into(),
                rows: batch(&[row]),
                weights: vec![1],
            }])
            .await
            .unwrap();
        data[1].push(row);
        assert_eq!(values(candidate.contents(), false), expected(1, &data));
        // The existing NULL compared value was already UNKNOWN; only value 2 changes.
        assert_eq!(candidate.delta().unwrap().num_rows(), 2);
        let mut peers = 0;
        for node in &candidate.stats().nodes {
            if let Some(work) = &node.keyed_work {
                assert_eq!(work.full_input_scans, 0);
                assert_eq!(
                    work.snapshot_evaluations_before + work.snapshot_evaluations_after,
                    0
                );
                // The first flag change also flows through the second IN's
                // member join, whose key includes both hot comparison values.
                assert!(work.touched_keys <= 2, "{}: {work:?}", node.operator);
                if let Some(work) = &work.match_join_work {
                    peers += work.peer_entries_visited;
                }
            }
        }
        assert!(peers > 0);
        eprintln!(
            "partition-scope untouched={untouched} match-peer-visits={peers} output-delta-rows={}",
            candidate.delta().unwrap().num_rows()
        );
        observed.push(peers);
    }
    assert_eq!(observed[0], observed[1]);
}

#[tokio::test]
async fn sql_correlated_truth_atomic_writes_retry_reopen_and_history_are_exact() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(p BIGINT,q BIGINT,v BIGINT);CREATE TABLE b(p BIGINT,q BIGINT,v BIGINT);INSERT INTO a VALUES (1,1,1),(1,1,2),(1,1,2),(2,2,NULL),(NULL,1,NULL),(1,NULL,NULL);INSERT INTO b VALUES (1,1,1),(2,2,2),(NULL,1,NULL),(1,NULL,NULL)").await.unwrap();
    let mut data = [
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(1), Some(2)),
            (Some(1), Some(1), Some(2)),
            (Some(2), Some(2), None),
            (None, Some(1), None),
            (Some(1), None, None),
        ],
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(2), Some(2), Some(2)),
            (None, Some(1), None),
            (Some(1), None, None),
        ],
    ];
    for mode in 0..4 {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW part_{mode} AS {}",
            query(mode)
        ))
        .await
        .unwrap();
    }
    for round in 0..8 {
        let sql = match round {
            0 => {
                data[1].extend([(Some(1), Some(1), None); 2]);
                "INSERT INTO b VALUES (1,1,NULL),(1,1,NULL)"
            }
            1 => {
                data[1].retain(|r| !(r.0 == Some(1) && r.1 == Some(1) && r.2.is_none()));
                "DELETE FROM b WHERE p=1 AND q=1 AND v IS NULL"
            }
            2 => {
                for r in &mut data[1] {
                    if r.0 == Some(1) && r.1 == Some(1) {
                        r.0 = Some(2);
                        r.1 = Some(2);
                    }
                }
                "UPDATE b SET p=2,q=2 WHERE p=1 AND q=1"
            }
            3 => {
                for r in &mut data[0] {
                    if r.0 == Some(1) && r.1 == Some(1) {
                        r.0 = Some(2);
                        r.1 = Some(2);
                    }
                }
                data[1].retain(|r| !(r.0 == Some(2) && r.1 == Some(2) && r.2 == Some(1)));
                data[0].extend([(Some(1), Some(1), Some(9)), (Some(2), Some(2), None)]);
                "UPDATE a SET p=2,q=2 WHERE p=1 AND q=1;DELETE FROM b WHERE p=2 AND q=2 AND v=1;INSERT INTO a VALUES (1,1,9),(2,2,NULL)"
            }
            4 => {
                data[1].retain(|r| !(r.0 == Some(2) && r.1 == Some(2)));
                "DELETE FROM b WHERE p=2 AND q=2"
            }
            5 => {
                data[1].extend([
                    (Some(2), Some(2), None),
                    (Some(2), Some(2), None),
                    (Some(1), Some(1), Some(9)),
                ]);
                "INSERT INTO b VALUES (2,2,NULL),(2,2,NULL),(1,1,9)"
            }
            6 => {
                data[0].clear();
                data[1].clear();
                "DELETE FROM a;DELETE FROM b"
            }
            7 => {
                data = [
                    vec![
                        (Some(1), Some(1), None),
                        (Some(1), Some(1), Some(1)),
                        (None, Some(1), None),
                    ],
                    vec![(Some(1), Some(1), None)],
                ];
                "INSERT INTO a VALUES (1,1,NULL),(1,1,1),(NULL,1,NULL);INSERT INTO b VALUES (1,1,NULL)"
            }
            _ => unreachable!("SQL round"),
        };
        let id = format!("partition-{round}");
        let epoch = db.epoch();
        let response = db.execute_transaction(&id, sql).await.unwrap();
        assert!(!response.replayed);
        assert_eq!(response.epoch, epoch + 1);
        let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
        for mode in 0..4 {
            let actual = db
                .execute(&format!("SELECT * FROM part_{mode}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(
                values(&actual, false),
                expected(mode, &data),
                "mode {mode} round {round}"
            );
            let read = db.execute(&query(mode)).await.unwrap().rows.unwrap();
            assert_eq!(read.schema(), actual.schema());
            assert_eq!(values(&read, false), values(&actual, false));
            let nodes = receipt["maintenance"][format!("part_{mode}")]["nodes"]
                .as_array()
                .unwrap();
            assert!(nodes.iter().all(|n| n["strategy"] != "OperatorRecompute"));
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_transaction(&id, sql).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.epoch, epoch + 1);
        assert_eq!(
            serde_json::to_value(retry.commit.unwrap()).unwrap(),
            receipt
        );
        for mode in 0..4 {
            let actual = db
                .execute(&format!("SELECT * FROM part_{mode}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(values(&actual, false), expected(mode, &data));
        }
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn correlated_dml_selectors_and_insert_select_preserve_empty_partition_truth() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(p BIGINT,q BIGINT,v BIGINT);CREATE TABLE b(p BIGINT,q BIGINT,v BIGINT);CREATE TABLE target(p BIGINT,q BIGINT,v BIGINT,m BOOLEAN,n BOOLEAN);INSERT INTO a VALUES (1,1,1),(1,1,2),(2,2,NULL),(NULL,1,NULL);INSERT INTO b VALUES (1,1,1),(1,1,NULL)").await.unwrap();
    let data = [
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(1), Some(1), Some(2)),
            (Some(2), Some(2), None),
            (None, Some(1), None),
        ],
        vec![(Some(1), Some(1), Some(1)), (Some(1), Some(1), None)],
    ];
    db.execute(&format!("INSERT INTO target {}", query(1)))
        .await
        .unwrap();
    let output = db
        .execute("SELECT * FROM target")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(values(&output, false), expected(1, &data));
    let predicate = "v=9 OR v NOT IN (SELECT b.v FROM b WHERE b.p=a.p AND b.q=a.q)";
    db.execute(&format!("DELETE FROM a WHERE {predicate}"))
        .await
        .unwrap();
    let output = db.execute("SELECT * FROM a").await.unwrap().rows.unwrap();
    assert_eq!(
        values(&output, false),
        values(
            &batch(&[(Some(1), Some(1), Some(1)), (Some(1), Some(1), Some(2))]),
            false
        )
    );
    db.execute("DELETE FROM b WHERE v IS NULL").await.unwrap();
    db.execute(&format!("UPDATE a SET v=7 WHERE {predicate}"))
        .await
        .unwrap();
    let output = db.execute("SELECT * FROM a").await.unwrap().rows.unwrap();
    assert_eq!(
        values(&output, false),
        values(
            &batch(&[(Some(1), Some(1), Some(1)), (Some(1), Some(1), Some(7))]),
            false
        )
    );
    db.execute("DELETE FROM b").await.unwrap();
    db.execute(&format!("DELETE FROM a WHERE {predicate}"))
        .await
        .unwrap();
    assert_eq!(
        db.execute("SELECT * FROM a")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        0
    );
}

#[tokio::test]
async fn correlated_nonnullable_values_keep_nonnullable_flags_with_nullable_keys() {
    use datafusion::prelude::SessionContext;
    let data = [
        vec![
            (Some(1), Some(1), Some(1)),
            (Some(2), Some(2), Some(2)),
            (None, Some(1), Some(1)),
        ],
        vec![(Some(1), Some(1), Some(1)), (None, Some(1), Some(1))],
    ];
    let source = tables(&data)
        .into_iter()
        .map(|(name, batch)| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("p", DataType::Int64, true),
                Field::new("q", DataType::Int64, true),
                Field::new("v", DataType::Int64, false),
            ]));
            (
                name,
                RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let ctx = SessionContext::new();
    for (name, batch) in &source {
        ctx.register_batch(name, batch.clone()).unwrap();
    }
    let original = ctx.state().create_logical_plan(&query(1)).await.unwrap();
    let view = SqlView::new_with_optimizer(
        &query(1),
        source,
        IncrementalOptimizer::default().require_incremental(),
    )
    .await
    .unwrap();
    let output = view.contents().unwrap();
    assert_eq!(output.schema().as_ref(), original.schema().as_arrow());
    assert!(!output.schema().field(3).is_nullable());
    assert!(!output.schema().field(4).is_nullable());
    assert_eq!(values(&output, false), expected(1, &data));
}

#[tokio::test]
async fn unproved_correlation_boundaries_refuse_without_publication_or_receipts() {
    use optd_incremental_sql::database::SqlSession;
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(p BIGINT,q BIGINT,v BIGINT);CREATE TABLE b(p BIGINT,q BIGINT,v BIGINT);INSERT INTO a VALUES (1,1,1);INSERT INTO b VALUES (1,1,1)").await.unwrap();
    let epoch = db.epoch();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    for (round, subquery) in [
        "SELECT MAX(b.v) FROM b WHERE b.p=a.p",
        "SELECT b.v FROM b WHERE b.p=a.p LIMIT 1",
        "SELECT DISTINCT b.v FROM b WHERE b.p=a.p",
        "SELECT b.v FROM b WHERE b.p IS NOT DISTINCT FROM a.p",
        "SELECT b.v FROM b WHERE b.p>=a.p",
        "SELECT b.v FROM b WHERE b.p=a.p+1",
        "SELECT b.v+a.v FROM b WHERE b.p=a.p",
    ]
    .into_iter()
    .enumerate()
    {
        let query = format!("SELECT a.v IN ({subquery}) AS m FROM a");
        let error = db
            .execute_request(
                &format!("boundary-{round}"),
                &format!("CREATE MATERIALIZED VIEW refused AS {query}"),
            )
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("three-valued"),
            "{query}: {error}"
        );
        assert_eq!(db.epoch(), epoch);
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
        assert!(db.explain_view("refused").is_err());
    }
    let response = db
        .execute_request(
            "boundary-0",
            "CREATE MATERIALIZED VIEW valid AS SELECT p FROM a",
        )
        .await
        .unwrap();
    assert!(!response.replayed);
}
