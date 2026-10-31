//! Default SQL registry, atomic writes and durable match-count reconstruction.

use optd_incremental_sql::database::SqlSession;

use super::{expected, full, values, Row};

/// Independent endpoint mutations accompany SQL rather than reading its target.
struct Round {
    sql: &'static str,
    edit: fn(&mut [Vec<Row>; 2]),
}

fn rounds() -> Vec<Round> {
    vec![
        Round {
            sql: "INSERT INTO b VALUES (1,20),(1,20),(2,5)",
            edit: |data| data[1].extend([(Some(1), Some(20)), (Some(1), Some(20)), (Some(2), Some(5))]),
        },
        Round {
            sql: "UPDATE a SET k=3,v=25 WHERE k=1",
            edit: |data| {
                for row in &mut data[0] {
                    if row.0 == Some(1) { *row = (Some(3), Some(25)); }
                }
            },
        },
        Round {
            sql: "DELETE FROM b WHERE k=1",
            edit: |data| data[1].retain(|row| row.0 != Some(1)),
        },
        Round {
            sql: "DELETE FROM a WHERE k=3; INSERT INTO a VALUES (1,7),(5,9); UPDATE b SET k=1,v=8 WHERE k=3",
            edit: |data| {
                data[0].retain(|row| row.0 != Some(3));
                data[0].extend([(Some(1), Some(7)), (Some(5), Some(9))]);
                for row in &mut data[1] {
                    if row.0 == Some(3) { *row = (Some(1), Some(8)); }
                }
            },
        },
        Round {
            sql: "UPDATE a SET v=0 WHERE k IS NULL; UPDATE b SET v=1 WHERE k IS NULL",
            edit: |data| {
                for (side, rows) in data.iter_mut().enumerate() {
                    for row in rows {
                        if row.0.is_none() { row.1 = Some(side as i64); }
                    }
                }
            },
        },
        Round { sql: "DELETE FROM b", edit: |data| data[1].clear() },
        Round {
            sql: "INSERT INTO b VALUES (1,8),(1,8),(NULL,1),(2,NULL)",
            edit: |data| data[1].extend([(Some(1), Some(8)), (Some(1), Some(8)), (None, Some(1)), (Some(2), None)]),
        },
        Round {
            sql: "DELETE FROM a; INSERT INTO a VALUES (NULL,NULL),(NULL,NULL),(1,7)",
            edit: |data| data[0] = vec![(None, None), (None, None), (Some(1), Some(7))],
        },
        Round {
            sql: "DELETE FROM a; DELETE FROM b",
            edit: |data| { data[0].clear(); data[1].clear(); },
        },
        Round {
            sql: "INSERT INTO a VALUES (NULL,NULL),(NULL,NULL),(1,7); INSERT INTO b VALUES (NULL,NULL),(NULL,NULL),(1,8)",
            edit: |data| {
                data[0] = vec![(None, None), (None, None), (Some(1), Some(7))];
                data[1] = vec![(None, None), (None, None), (Some(1), Some(8))];
            },
        },
    ]
}

/// Compare the base join with a separate nested-loop model and each consumer
/// with fresh full SQL. Retrying after reopen must preserve the original receipt.
#[tokio::test]
async fn default_sql_left_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("LEFT").await;
}

#[tokio::test]
async fn default_sql_right_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("RIGHT").await;
}

#[tokio::test]
async fn default_sql_full_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("FULL").await;
}

#[tokio::test]
async fn default_sql_left_semi_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("LEFT SEMI").await;
}

#[tokio::test]
async fn default_sql_right_semi_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("RIGHT SEMI").await;
}

#[tokio::test]
async fn default_sql_left_anti_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("LEFT ANTI").await;
}

#[tokio::test]
async fn default_sql_right_anti_dml_atomic_retry_history_and_reopen_match_oracles() {
    qualify_sql_kind("RIGHT ANTI").await;
}

async fn qualify_sql_kind(kind: &str) {
    for (mode, condition) in [
        (0, "a.k=b.k"),
        (1, "a.k IS NOT DISTINCT FROM b.k"),
        (3, "a.k=b.k AND a.v<b.v"),
        (4, "(a.k IS NOT DISTINCT FROM b.k) AND a.v<b.v"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = SqlSession::open(root.path()).await.unwrap();
        db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT); CREATE TABLE b(k BIGINT,v BIGINT); INSERT INTO a VALUES (1,10),(1,10),(NULL,NULL),(2,NULL),(4,40); INSERT INTO b VALUES (1,15),(NULL,NULL),(3,30),(4,NULL)").await.unwrap();
        let mut data = [
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (None, None),
                (Some(2), None),
                (Some(4), Some(40)),
            ],
            vec![
                (Some(1), Some(15)),
                (None, None),
                (Some(3), Some(30)),
                (Some(4), None),
            ],
        ];
        let (projection, key) = if kind.starts_with("LEFT ") {
            ("a.k AS ak,a.v AS av", "ak")
        } else if kind.starts_with("RIGHT ") {
            ("b.k AS bk,b.v AS bv", "bk")
        } else {
            ("a.k AS ak,a.v AS av,b.k AS bk,b.v AS bv", "coalesce(ak,bk)")
        };
        let query = format!("SELECT {projection} FROM a {kind} JOIN b ON {condition}");
        let queries = [
            ("j", query.clone()),
            (
                "g",
                format!("SELECT {key} AS k,count(*) AS n FROM ({query}) q GROUP BY {key}"),
            ),
            (
                "w",
                format!("SELECT q.*,count(*) OVER (PARTITION BY {key}) AS n FROM ({query}) q"),
            ),
        ];
        for (name, sql) in &queries {
            db.execute(&format!("CREATE MATERIALIZED VIEW {name} AS {sql}"))
                .await
                .unwrap();
            let plan = db.explain_view(name).unwrap();
            assert!(plan.contains("PhysicalMaintainMatchJoin"), "{plan}");
        }
        check(&mut db, &queries, kind, mode, &data).await;
        for (round, step) in rounds().into_iter().enumerate() {
            let id = format!("join-{round}");
            let epoch = db.epoch();
            let response = if step.sql.contains(';') {
                db.execute_transaction(&id, step.sql).await
            } else {
                db.execute_request(&id, step.sql).await
            }
            .unwrap();
            assert!(!response.replayed);
            assert_eq!(response.epoch, epoch + 1);
            let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
            for (name, _) in &queries {
                let nodes = receipt["maintenance"][name]["nodes"].as_array().unwrap();
                let joins = nodes
                    .iter()
                    .filter(|node| node["strategy"] == "MatchCountDelta")
                    .collect::<Vec<_>>();
                // A key-only consumer can consolidate a value UPDATE to zero
                // at both scans. Its selected join then needs no execution.
                if joins.is_empty() {
                    assert!(
                        nodes.iter().all(|node| node["delta_rows"] == 0),
                        "unexplained skipped join: {kind} mode {mode} round {round}: {nodes:?}"
                    );
                    continue;
                }
                assert_eq!(joins.len(), 1);
                let work = &joins[0]["keyed_work"];
                assert_eq!(work["snapshot_evaluations_before"], 0);
                assert_eq!(work["snapshot_evaluations_after"], 0);
                assert_eq!(work["full_input_scans"], 0);
                assert!(work["match_join_work"].is_object());
                assert!(nodes
                    .iter()
                    .all(|node| node["strategy"] != "OperatorRecompute"));
            }
            (step.edit)(&mut data);
            check(&mut db, &queries, kind, mode, &data).await;
            db.verify_output_history().unwrap();
            let epoch = db.epoch();
            drop(db);
            db = SqlSession::open(root.path()).await.unwrap();
            assert_eq!(db.epoch(), epoch);
            let retry = if step.sql.contains(';') {
                db.execute_transaction(&id, step.sql).await
            } else {
                db.execute_request(&id, step.sql).await
            }
            .unwrap();
            assert!(retry.replayed);
            assert_eq!(retry.epoch, epoch);
            assert_eq!(
                serde_json::to_value(retry.commit.unwrap()).unwrap(),
                receipt
            );
            check(&mut db, &queries, kind, mode, &data).await;
            db.verify_output_history().unwrap();
        }
    }
}

async fn check(
    db: &mut SqlSession,
    queries: &[(&str, String)],
    kind: &str,
    mode: usize,
    data: &[Vec<Row>; 2],
) {
    for (name, query) in queries {
        let actual = db
            .execute(&format!("SELECT * FROM {name}"))
            .await
            .unwrap()
            .rows
            .unwrap();
        let reference = full(query, data).await;
        assert_eq!(actual.schema(), reference.schema(), "{query}");
        assert_eq!(
            values(&actual, false),
            values(&reference, false),
            "full SQL {query}"
        );
        if *name == "j" {
            assert_eq!(
                values(&actual, false),
                expected(kind, mode, &data[0], &data[1]),
                "independent {query}"
            );
        }
    }
}
