//! Mixed correlation policies must not change the scalar comparison's SQL equality.

use super::correlated::{batch, tables, Row};
use super::{add, membership, text, values, Rows};
use optd_incremental_sql::database::SqlSession;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, TableChange};

fn equal(left: Option<i64>, right: Option<i64>, null_safe: bool) -> bool {
    left == right && (null_safe || left.is_some())
}

fn query(policy: usize) -> String {
    let p = if policy & 1 != 0 {
        "IS NOT DISTINCT FROM"
    } else {
        "="
    };
    let q = if policy & 2 != 0 {
        "IS NOT DISTINCT FROM"
    } else {
        "="
    };
    let subquery = format!("SELECT b.v FROM b WHERE (b.p {p} a.p) AND (b.q {q} a.q)");
    format!("SELECT a.p,a.q,a.v,a.v IN ({subquery}) AS m,a.v NOT IN ({subquery}) AS n FROM a")
}

fn expected(policy: usize, data: &[Vec<Row>; 2]) -> Rows {
    let mut result = Rows::new();
    for a in &data[0] {
        let peers = data[1]
            .iter()
            .filter(|b| equal(a.0, b.0, policy & 1 != 0) && equal(a.1, b.1, policy & 2 != 0))
            .map(|b| b.2)
            .collect::<Vec<_>>();
        let truth = membership(a.2, &peers);
        add(
            &mut result,
            vec![
                text(a.0),
                text(a.1),
                text(a.2),
                truth.map_or("NULL".into(), |v| v.to_string()),
                truth.map(|v| !v).map_or("NULL".into(), |v| v.to_string()),
            ],
            1,
        );
    }
    result
}

#[tokio::test]
async fn all_correlation_null_policies_match_independent_signed_prefixes() {
    for policy in 0..4 {
        let mut data = [
            vec![
                (None, None, Some(1)),
                (None, None, Some(2)),
                (None, None, None),
                (Some(1), None, Some(2)),
                (None, Some(1), None),
                (Some(1), Some(1), Some(1)),
            ],
            vec![(None, None, Some(1)), (Some(1), Some(1), Some(1))],
        ];
        let mut view = SqlView::new_with_optimizer(
            &query(policy),
            tables(&data),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let mut replay = values(&view.contents().unwrap(), false);
        assert_eq!(replay, expected(policy, &data));
        let directed = [
            ((None, None, None), 1),
            ((None, None, None), 1),
            ((None, None, None), -1),
            ((None, None, None), -1),
            ((None, None, Some(1)), -1),
            ((None, Some(1), None), 1),
        ];
        let mut seed = 0x134d_u64;
        for round in 0..32 {
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
                    let p = (seed >> 20) % 3;
                    let q = (seed >> 24) % 3;
                    let v = (seed >> 30) % 4;
                    (
                        (
                            (p != 2).then_some(p as i64),
                            (q != 2).then_some(q as i64),
                            (v != 3).then_some(v as i64),
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
            let before = view.contents().unwrap();
            let discarded = view.prepare(&changes).await.unwrap();
            assert_eq!(values(discarded.contents(), false), expected(policy, &data));
            drop(discarded);
            assert_eq!(view.contents().unwrap(), before);
            let candidate = view.prepare(&changes).await.unwrap();
            for (row, weight) in values(&candidate.delta().unwrap(), true) {
                add(&mut replay, row, weight);
            }
            assert_eq!(
                replay,
                expected(policy, &data),
                "policy {policy} round {round}"
            );
            view.acknowledge(candidate).unwrap();
        }
    }
}

#[tokio::test]
async fn mixed_null_policy_scope_is_independent_of_untouched_partitions() {
    for policy in 1..4 {
        let mut visits = Vec::new();
        for untouched in [8, 512] {
            let p = if policy & 1 != 0 { None } else { Some(1) };
            let q = if policy & 2 != 0 { None } else { Some(1) };
            let mut data = [vec![(p, q, Some(2)), (p, q, None)], vec![(p, q, Some(1))]];
            for n in 2..untouched {
                data[0].push((Some(n), Some(n), Some(2)));
                data[1].push((Some(n), Some(n), Some(1)));
            }
            let view = SqlView::new_with_optimizer(
                &query(policy),
                tables(&data),
                IncrementalOptimizer::default().require_incremental(),
            )
            .await
            .unwrap();
            let row = (p, q, None);
            data[1].push(row);
            let candidate = view
                .prepare(&[TableChange {
                    table: "b".into(),
                    rows: batch(&[row]),
                    weights: vec![1],
                }])
                .await
                .unwrap();
            assert_eq!(values(candidate.contents(), false), expected(policy, &data));
            assert_eq!(candidate.delta().unwrap().num_rows(), 2);
            let mut peers = 0;
            for node in &candidate.stats().nodes {
                if let Some(work) = &node.keyed_work {
                    assert_eq!(work.full_input_scans, 0);
                    assert_eq!(
                        work.snapshot_evaluations_before + work.snapshot_evaluations_after,
                        0
                    );
                    if let Some(work) = &work.match_join_work {
                        peers += work.peer_entries_visited;
                    }
                }
            }
            assert!(peers > 0);
            visits.push(peers);
        }
        assert_eq!(visits[0], visits[1], "policy {policy}");
    }
}

#[tokio::test]
async fn sql_null_policies_recover_exact_partition_truth_and_request_identity() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script("CREATE TABLE a(p BIGINT,q BIGINT,v BIGINT);CREATE TABLE b(p BIGINT,q BIGINT,v BIGINT);INSERT INTO a VALUES (NULL,NULL,1),(NULL,NULL,2),(1,NULL,2),(NULL,1,NULL),(1,1,1);INSERT INTO b VALUES (NULL,NULL,1),(1,1,1)").await.unwrap();
    let mut data = [
        vec![
            (None, None, Some(1)),
            (None, None, Some(2)),
            (Some(1), None, Some(2)),
            (None, Some(1), None),
            (Some(1), Some(1), Some(1)),
        ],
        vec![(None, None, Some(1)), (Some(1), Some(1), Some(1))],
    ];
    for policy in 0..4 {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW policy_{policy} AS {}",
            query(policy)
        ))
        .await
        .unwrap();
    }
    for round in 0..6 {
        let sql = match round {
            0 => {
                data[1].extend([
                    (None, None, None),
                    (Some(1), None, None),
                    (None, Some(1), None),
                ]);
                "INSERT INTO b VALUES (NULL,NULL,NULL),(1,NULL,NULL),(NULL,1,NULL)"
            }
            1 => {
                data[1].retain(|r| r.2.is_some());
                "DELETE FROM b WHERE v IS NULL"
            }
            2 => {
                for r in &mut data[1] {
                    if r.0.is_none() {
                        r.0 = Some(1);
                        r.1 = Some(1);
                    }
                }
                "UPDATE b SET p=1,q=1 WHERE p IS NULL"
            }
            3 => {
                for r in &mut data[0] {
                    if r.0.is_none() {
                        r.0 = Some(1);
                    }
                }
                data[1].push((Some(1), None, None));
                data[1].retain(|r| !(r.0 == Some(1) && r.1 == Some(1)));
                "UPDATE a SET p=1 WHERE p IS NULL;INSERT INTO b VALUES (1,NULL,NULL);DELETE FROM b WHERE p=1 AND q=1"
            }
            4 => {
                data[1].clear();
                "DELETE FROM b"
            }
            5 => {
                data[1].push((None, None, None));
                data[0].push((None, None, None));
                "INSERT INTO b VALUES (NULL,NULL,NULL);INSERT INTO a VALUES (NULL,NULL,NULL)"
            }
            _ => unreachable!("SQL round"),
        };
        let id = format!("policy-{round}");
        let response = db.execute_transaction(&id, sql).await.unwrap();
        assert!(!response.replayed);
        let epoch = response.epoch;
        let receipt = serde_json::to_value(response.commit.unwrap()).unwrap();
        for policy in 0..4 {
            let output = db
                .execute(&format!("SELECT * FROM policy_{policy}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(values(&output, false), expected(policy, &data));
        }
        db.verify_output_history().unwrap();
        drop(db);
        db = SqlSession::open(root.path()).await.unwrap();
        let retry = db.execute_transaction(&id, sql).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.epoch, epoch);
        assert_eq!(
            serde_json::to_value(retry.commit.unwrap()).unwrap(),
            receipt
        );
        for policy in 0..4 {
            let output = db
                .execute(&format!("SELECT * FROM policy_{policy}"))
                .await
                .unwrap()
                .rows
                .unwrap();
            assert_eq!(values(&output, false), expected(policy, &data));
        }
        db.verify_output_history().unwrap();
    }
}
