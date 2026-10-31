//! Changed-block work must stay bounded while unaffected table blocks grow.

use optd_incremental_sql::database::{display_rows, SqlSession};
use serde_json::Value;

async fn seeded(root: &std::path::Path, n: usize) -> SqlSession {
    let mut db = SqlSession::open(root).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT, v BIGINT)")
        .await
        .unwrap();
    let rows = (0..n).map(|k| format!("({k},{k})")).collect::<Vec<_>>();
    db.execute(&format!("INSERT INTO t VALUES {}", rows.join(",")))
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn fixed_write_copies_only_one_bounded_leaf_as_untouched_blocks_grow() {
    for n in [1024, 8192] {
        let root = tempfile::tempdir().unwrap();
        let mut db = seeded(root.path(), n).await;
        let before = db.store_work().unwrap();
        let response = db.execute("INSERT INTO t VALUES (-1,-1)").await.unwrap();
        let w = serde_json::to_value(&response.commit.unwrap().dml[0]).unwrap();
        let target = &w["execution"]["target"];
        assert_eq!(w["execution"]["selector"]["source_rows"], 0);
        assert_eq!(target["collected_rows"], 0);
        assert_eq!(target["distinct_entries"], 0);
        assert_eq!(target["block_update"]["touched_blocks"], 1);
        let copies = target["block_update"]["directory_entries_copied"]
            .as_u64()
            .unwrap() as usize;
        assert!(copies > 0 && copies <= 4 * ((n / 256).ilog2() as usize + 1));
        assert_eq!(target["delta_update"]["old_row_entries_copied"], 256);
        assert_eq!(target["delta_update"]["materialized_rows"], 257);
        // Full checkpoint flattening remains separate from statement-local leaf work.
        let storage =
            serde_json::to_value(db.store_work().unwrap().since(&before).unwrap()).unwrap();
        assert_eq!(storage["table_materialization"]["rows"], n + 1);
        assert_eq!(storage["tables"]["written"]["rows"], n + 1);
        let actual = db
            .execute("SELECT k,v FROM t ORDER BY k")
            .await
            .unwrap()
            .rows
            .unwrap();
        let expected = (-1..n as i64)
            .map(|k| vec![k.to_string(), k.to_string()])
            .collect::<Vec<_>>();
        assert_eq!(display_rows(&actual).unwrap(), expected);
    }
}

#[tokio::test]
async fn no_op_and_canceled_statements_share_table_blocks_without_flattening() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path(), 1024).await;
    let before = db.store_work().unwrap();
    let response = db.execute("UPDATE t SET v=v").await.unwrap();
    let w = serde_json::to_value(&response.commit.unwrap().dml[0]).unwrap();
    assert_eq!(
        w["execution"]["target"]["delta_update"]["materialized_rows"],
        0
    );
    assert_eq!(
        w["execution"]["target"]["block_update"]["directory_entries_copied"],
        0
    );
    let storage = serde_json::to_value(db.store_work().unwrap().since(&before).unwrap()).unwrap();
    assert_eq!(storage["table_materialization"]["rows"], 0);
    assert_eq!(storage["tables"]["written"]["rows"], 0);
    db.execute("BEGIN").await.unwrap();
    db.execute("INSERT INTO t VALUES (-1,10)").await.unwrap();
    db.execute("DELETE FROM t WHERE k=-1").await.unwrap();
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM t")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        1
    );
    db.execute("ROLLBACK").await.unwrap();
    assert_eq!(
        db.execute("SELECT * FROM t WHERE k=-1")
            .await
            .unwrap()
            .rows
            .unwrap()
            .num_rows(),
        0
    );
}

#[tokio::test]
async fn split_key_moves_duplicates_nulls_and_failed_transactions_preserve_endpoints() {
    let root = tempfile::tempdir().unwrap();
    let mut db = seeded(root.path(), 768).await;
    db.execute(
        "CREATE MATERIALIZED VIEW counts AS SELECT k,COUNT(*) AS n,SUM(v) AS s FROM t GROUP BY k",
    )
    .await
    .unwrap();
    db.execute("BEGIN").await.unwrap();
    db.execute("INSERT INTO t VALUES (NULL,8),(0,0),(0,0)")
        .await
        .unwrap();
    db.execute("UPDATE t SET k=v,v=k WHERE k=0").await.unwrap();
    db.execute("DELETE FROM t WHERE k=256 OR k=511")
        .await
        .unwrap();
    db.execute("UPDATE t SET k=-k WHERE k=767").await.unwrap();
    let report = db.execute("COMMIT").await.unwrap().commit.unwrap();
    assert_eq!(report.dml.len(), 4);
    let mut expected = (0..768)
        .filter(|k| *k != 256 && *k != 511)
        .map(|k| vec![if k == 767 { -767 } else { k }.to_string(), k.to_string()])
        .collect::<Vec<_>>();
    expected.extend([
        vec!["NULL".into(), "8".into()],
        vec!["0".into(), "0".into()],
        vec!["0".into(), "0".into()],
    ]);
    expected.sort();
    let mut actual =
        display_rows(&db.execute("SELECT * FROM t").await.unwrap().rows.unwrap()).unwrap();
    actual.sort();
    assert_eq!(actual, expected);
    db.verify_output_history().unwrap();
    let epoch = db.epoch();
    db.execute("BEGIN").await.unwrap();
    db.execute("DELETE FROM t WHERE k=0").await.unwrap();
    assert!(db.execute("UPDATE t SET k=1/0").await.is_err());
    assert!(db.execute("COMMIT").await.is_err());
    db.execute("ROLLBACK").await.unwrap();
    assert_eq!(db.epoch(), epoch);
    drop(db);
    let mut db = SqlSession::open(root.path()).await.unwrap();
    let mut actual =
        display_rows(&db.execute("SELECT * FROM t").await.unwrap().rows.unwrap()).unwrap();
    actual.sort();
    assert_eq!(actual, expected);
    db.verify_output_history().unwrap();
    let count = db
        .execute("SELECT n,s FROM counts WHERE k=0")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(
        display_rows(&count).unwrap(),
        vec![vec!["3".to_owned(), "0".to_owned()]]
    );
    let legacy: Value = serde_json::to_value(report).unwrap();
    assert_eq!(
        legacy["dml"][1]["execution"]["target"]["block_update"]["directory_entries_copied"],
        0
    );
}
