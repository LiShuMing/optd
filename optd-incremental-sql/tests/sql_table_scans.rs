//! SQL statement snapshots expose actual lazy scan and checkpoint demand separately.

use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::RowReadWork;

#[tokio::test]
async fn fixed_transaction_prefix_reads_do_not_expand_untouched_tables_or_duplicate_weights() {
    for count in [1024, 8192] {
        let root = tempfile::tempdir().unwrap();
        let mut session = SqlSession::open(root.path()).await.unwrap();
        session
            .execute("CREATE TABLE t(k BIGINT, payload VARCHAR)")
            .await
            .unwrap();
        let values = (0..count)
            .map(|k| format!("({k},'payload')"))
            .collect::<Vec<_>>();
        session
            .execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
            .await
            .unwrap();
        session
            .execute("CREATE TABLE unused(k BIGINT)")
            .await
            .unwrap();
        session
            .execute("INSERT INTO unused VALUES (10),(20)")
            .await
            .unwrap();
        session
            .execute("CREATE MATERIALIZED VIEW v AS SELECT sum(k) AS s FROM t")
            .await
            .unwrap();
        session.execute("BEGIN").await.unwrap();
        session
            .execute("INSERT INTO t VALUES (-1,'payload')")
            .await
            .unwrap();
        let response = session.execute("SELECT k FROM t LIMIT 1").await.unwrap();
        assert_eq!(
            display_rows(&response.rows.unwrap()).unwrap(),
            vec![vec!["-1"]]
        );
        let reads = response.table_reads.unwrap();
        assert_eq!(reads.leaf_chunks, 1);
        assert_eq!(reads.materialization.materialized_rows, 1);
        assert_eq!(reads.stored_entries_visited, 1);
        assert_eq!(reads.ordering_rows, 256);
        for query in [
            "SELECT count(*) FROM t",
            "SELECT count(k) FROM t",
            "SELECT k FROM t WHERE false",
            "SELECT k FROM t LIMIT 0",
        ] {
            let response = session.execute(query).await.unwrap();
            assert_eq!(
                response.table_reads,
                Some(RowReadWork::default()),
                "{query}"
            );
        }
        let before = session.store_work().unwrap();
        let committed = session.execute("COMMIT").await.unwrap().commit.unwrap();
        let target = &committed.dml[0].execution.as_ref().unwrap().target;
        assert_eq!(target.delta_update.materialized_rows, 0);
        assert_eq!(target.delta_update.old_row_entries_copied, 256);
        assert_eq!(target.leaf_validation.as_ref().unwrap().rows, 257);
        let storage = session.store_work().unwrap().since(&before).unwrap();
        assert_eq!(
            storage
                .table_leaf_demand
                .as_ref()
                .unwrap()
                .materialized_rows,
            257
        );
        assert_eq!(
            storage.table_materialization.as_ref().unwrap().rows,
            count as u64 + 1
        );
        assert_eq!(storage.tables.written.rows, count as u64 + 1);
        let response = session.execute("SELECT * FROM v").await.unwrap();
        assert_eq!(response.table_reads, Some(RowReadWork::default()));
        let expected = count * (count - 1) / 2 - 1;
        assert_eq!(
            display_rows(&response.rows.unwrap()).unwrap(),
            vec![vec![expected.to_string()]]
        );
        drop(session);
        let mut session = SqlSession::open(root.path()).await.unwrap();
        let recovery = &session.recovery_work().store;
        assert_eq!(
            recovery.table_bootstrap.as_ref().unwrap().materialized_rows,
            0
        );
        assert!(recovery.table_leaf_validation.as_ref().unwrap().rows > count);
        let response = session.execute("SELECT k FROM t LIMIT 1").await.unwrap();
        assert_eq!(
            response
                .table_reads
                .unwrap()
                .materialization
                .materialized_rows,
            1
        );
        session.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn sql_duplicate_prefix_is_bounded_and_old_diagnostics_remain_unknown() {
    let root = tempfile::tempdir().unwrap();
    let mut session = SqlSession::open(root.path()).await.unwrap();
    session.execute("CREATE TABLE t(k BIGINT)").await.unwrap();
    let values = vec!["(1)"; 32_768].join(",");
    session
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .await
        .unwrap();
    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    for _ in 0..2 {
        let response = session.execute("SELECT k FROM t LIMIT 1").await.unwrap();
        let work = response.table_reads.unwrap();
        assert_eq!(work.materialization.materialized_rows, 1);
        assert_eq!(work.stored_entries_visited, 1);
        assert_eq!(work.cache_hits, 0);
    }
    session.execute("ROLLBACK").await.unwrap();
    let response = session
        .execute_request("once", "INSERT INTO t VALUES (NULL)")
        .await
        .unwrap();
    let original = serde_json::to_value(response.commit.unwrap()).unwrap();
    let mut old = original["dml"][0].clone();
    old["execution"]["selector"]
        .as_object_mut()
        .unwrap()
        .remove("table_reads");
    old["execution"]["target"]
        .as_object_mut()
        .unwrap()
        .remove("leaf_validation");
    let decoded: optd_incremental_sql::database::DmlWork = serde_json::from_value(old).unwrap();
    let decoded = decoded.execution.unwrap();
    assert!(decoded.selector.table_reads.is_none());
    assert!(decoded.target.leaf_validation.is_none());
    drop(session);
    let mut session = SqlSession::open(root.path()).await.unwrap();
    let response = session
        .execute_request("once", "INSERT INTO t VALUES (NULL)")
        .await
        .unwrap();
    assert!(response.replayed);
    assert_eq!(
        serde_json::to_value(response.commit.unwrap()).unwrap(),
        original
    );
}
