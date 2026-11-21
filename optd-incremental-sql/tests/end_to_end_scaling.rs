//! End-to-end scaling and work-scope regressions across growing untouched tables (R10a).
//!
//! Holds a single-row change fixed while untouched base-table cardinality grows from
//! 1,024 -> 8,192 -> 65,536 rows, independently asserting:
//! 1. Operator state copy and aggregate/join/rank delta work bounds.
//! 2. Precompiled template execution and zero full-table flattening.
//! 3. Block-level storage commit writes (<= 2 blocks / <= 512 rows, independent of N).
//! 4. Incremental operator-state recovery (`rebuilt_views == 0`, `dependency_rows == 0`).
//! 5. Honest reporting of unindexed DELETE/UPDATE selector scans (not faked as zero).

use std::fs;
use std::path::Path;

use optd_incremental_sql::database::{
    display_rows, IncompatibleStatePolicy, OpenOptions, SqlSession,
};
use serde_json::Value;

async fn seed_table(db: &mut SqlSession, n: usize) {
    db.execute("CREATE TABLE items(k BIGINT NOT NULL, grp BIGINT NOT NULL, v BIGINT NOT NULL)")
        .await
        .unwrap();
    db.execute("BEGIN").await.unwrap();
    // Chunk INSERT statements at 1,024 rows to stay within the expression binder budget.
    for start in (0..n).step_by(1024) {
        let end = (start + 1024).min(n);
        let values = (start..end)
            .map(|i| format!("({i},{},{i})", i % 64))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(&format!("INSERT INTO items VALUES {values}"))
            .await
            .unwrap();
    }
    db.execute("COMMIT").await.unwrap();
}

#[tokio::test]
async fn fixed_single_row_writes_remain_bounded_across_1k_8k_65k_rows() {
    for n in [1024usize, 8192, 65_536] {
        let root = tempfile::tempdir().unwrap();
        let mut db = SqlSession::open(root.path()).await.unwrap();
        seed_table(&mut db, n).await;
        db.execute(
            "CREATE MATERIALIZED VIEW agg_mv AS \
             SELECT grp, COUNT(*) AS cnt, SUM(v) AS total, MIN(v) AS lo, MAX(v) AS hi, \
                    COUNT(DISTINCT v) AS dcnt \
             FROM items GROUP BY grp",
        )
        .await
        .unwrap();

        // 1. Single-row INSERT: O(1) selector, O(1) operator work, O(log N) directory copies,
        //    O(1) block writes (<= 2 blocks / <= 512 rows) and zero full-table flattening.
        let before_store = db.store_work().unwrap();
        let ins = db
            .execute("INSERT INTO items VALUES (-1, 0, -1)")
            .await
            .unwrap()
            .commit
            .unwrap();
        let ins_store = db.store_work().unwrap().since(&before_store).unwrap();
        let ins_dml = &ins.dml[0].execution.as_ref().unwrap();
        assert_eq!(ins_dml.selector.source_rows, 0);
        assert_eq!(ins_store.table_materialization.as_ref().unwrap().misses, 0);
        assert_eq!(ins_store.table_materialization.as_ref().unwrap().rows, 0);
        assert!(
            (1..=2).contains(&ins_store.tables.written.files),
            "N={n}: expected <=2 written table blocks, got {}",
            ins_store.tables.written.files
        );
        assert!(
            ins_store.tables.written.rows <= 512,
            "N={n}: written table rows {} exceeded 2-block bound",
            ins_store.tables.written.rows
        );
        assert!(
            ins_store.tables.reused.rows >= (n as u64).saturating_sub(256),
            "N={n}: untouched table rows must be reused"
        );
        let maint = &ins.maintenance["agg_mv"];
        let template_exec = &maint["work"]["template_execution"];
        assert!(template_exec["direct_invocations"].as_u64().unwrap() > 0);
        assert_eq!(template_exec["fallback_invocations"].as_u64().unwrap(), 0);
        let agg_node = maint["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["accumulator_work"].is_object())
            .expect("expected accumulator node");
        assert_eq!(agg_node["accumulator_work"]["touched_groups"], 1);
        assert_eq!(agg_node["accumulator_work"]["delta_input_rows"], 1);
        assert_eq!(agg_node["accumulator_work"]["historical_input_rows"], 0);

        // 2. Single-row UPDATE: selector scan is honestly O(N) until DML indexing lands,
        //    while storage writes and operator maintenance stay O(1) bounded.
        let before_store = db.store_work().unwrap();
        let upd = db
            .execute("UPDATE items SET v = -2 WHERE k = -1")
            .await
            .unwrap()
            .commit
            .unwrap();
        let upd_store = db.store_work().unwrap().since(&before_store).unwrap();
        assert_eq!(
            upd.dml[0].execution.as_ref().unwrap().selector.source_rows,
            n + 1,
            "unindexed UPDATE selector must report actual full-table scan honestly"
        );
        assert!(upd_store.tables.written.files <= 2);
        assert!(upd_store.tables.written.rows <= 512);
        assert_eq!(upd_store.table_materialization.as_ref().unwrap().misses, 0);

        // 3. Single-row DELETE restoring original sum for grp=0.
        let before_store = db.store_work().unwrap();
        let del = db
            .execute("DELETE FROM items WHERE k = -1")
            .await
            .unwrap()
            .commit
            .unwrap();
        let del_store = db.store_work().unwrap().since(&before_store).unwrap();
        assert_eq!(
            del.dml[0].execution.as_ref().unwrap().selector.source_rows,
            n + 1,
            "unindexed DELETE selector must report actual full-table scan honestly"
        );
        assert!(del_store.tables.written.files <= 2);
        assert!(del_store.tables.written.rows <= 512);
        assert_eq!(del_store.table_materialization.as_ref().unwrap().misses, 0);

        // 4. Reopen must restore incrementally with zero full rebuilds and zero dependency scans.
        drop(db);
        let mut db = SqlSession::open(root.path()).await.unwrap();
        let rec = db.recovery_work();
        assert_eq!(rec.rebuilt_views, 0, "N={n}: must not rebuild view");
        assert_eq!(rec.dependency_rows, 0, "N={n}: must not scan dependencies");
        assert_eq!(rec.restored_views, Some(1));
        assert_eq!(rec.replayed_epochs, Some(3));
        // Independent integer arithmetic check for group 0 (0, 64, 128, ..., n-64).
        let group_count = (n / 64) as i64;
        let expected_sum = 64 * (group_count - 1) * group_count / 2;
        let expected_max = (group_count - 1) * 64;
        let actual = db
            .execute("SELECT cnt, total, lo, hi, dcnt FROM agg_mv WHERE grp = 0")
            .await
            .unwrap()
            .rows
            .unwrap();
        assert_eq!(
            display_rows(&actual).unwrap(),
            vec![vec![
                group_count.to_string(),
                expected_sum.to_string(),
                "0".to_owned(),
                expected_max.to_string(),
                group_count.to_string(),
            ]],
            "N={n}: independent arithmetic oracle mismatch"
        );
        db.verify_output_history().unwrap();
    }
}

#[tokio::test]
async fn o1_receipt_index_avoids_history_traversal_after_twenty_commits() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute("CREATE TABLE t(k BIGINT)").await.unwrap();
    let first = db
        .execute_request("req-first", "INSERT INTO t VALUES (1)")
        .await
        .unwrap()
        .commit
        .unwrap();
    for i in 2..=21 {
        db.execute(&format!("INSERT INTO t VALUES ({i})"))
            .await
            .unwrap();
    }
    drop(db);

    let mut db = SqlSession::open(root.path()).await.unwrap();
    let before = db.store_work().unwrap();
    let retry = db
        .execute_request("req-first", "INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    let after = db.store_work().unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.epoch, first.epoch);
    assert_eq!(after.history_traversals, before.history_traversals);
    assert_eq!(after.manifest_reads, before.manifest_reads);
    assert!(db
        .execute_request("req-first", "INSERT INTO t VALUES (999)")
        .await
        .is_err());
}

#[tokio::test]
async fn corrupted_operator_state_ipc_fails_closed_and_rebuilds_explicitly() {
    let root = tempfile::tempdir().unwrap();
    let mut db = SqlSession::open(root.path()).await.unwrap();
    db.execute_script(
        "CREATE TABLE t(k BIGINT, v BIGINT);
         INSERT INTO t VALUES (1, 10), (2, 20);
         CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS s FROM t GROUP BY k;",
    )
    .await
    .unwrap();
    drop(db);

    let pointer: Value =
        serde_json::from_slice(&fs::read(root.path().join("CURRENT")).unwrap()).unwrap();
    let dir = root.path().join(pointer["directory"].as_str().unwrap());
    let manifest: Value =
        serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    let state_ref = &manifest["view_states"]["mv"];
    let state_dir = root
        .path()
        .join(state_ref["directory"].as_str().unwrap())
        .join(state_ref["state_subdir"].as_str().unwrap());
    let op_manifest: Value = serde_json::from_slice(
        &fs::read(state_dir.join(state_ref["operator_manifest"]["name"].as_str().unwrap()))
            .unwrap(),
    )
    .unwrap();
    let outputs_file = op_manifest["operators"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|entry| {
            entry
                .get("Accumulator")?
                .get("outputs_file")?
                .get("name")?
                .as_str()
        })
        .expect("accumulator outputs_file");
    let target: &Path = &state_dir.join(outputs_file);
    fs::write(target, b"corrupted operator state bytes").unwrap();

    // Fail-closed by default.
    assert!(SqlSession::open(root.path()).await.is_err());

    // Opt-in Rebuild succeeds and explicitly attributes rebuilt_views == 1.
    let mut rebuilt = SqlSession::open_with_options(
        root.path(),
        OpenOptions {
            checkpoint_interval: 16,
            incompatible_state: IncompatibleStatePolicy::Rebuild,
        },
    )
    .await
    .unwrap();
    assert_eq!(rebuilt.recovery_work().rebuilt_views, 1);
    assert_eq!(rebuilt.recovery_work().restored_views, Some(0));
    let mut rows = display_rows(
        &rebuilt
            .execute("SELECT k, s FROM mv")
            .await
            .unwrap()
            .rows
            .unwrap(),
    )
    .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()],
        ]
    );
}
