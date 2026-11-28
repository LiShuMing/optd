//! Tests for history retention and reachability-protected GC compaction (`R07c` / RFC TODO-S3).

use std::fs;
use std::path::Path;

use optd_incremental_sql::database::{
    IncompatibleStatePolicy, OpenOptions, RetentionPolicy, SqlSession,
};
use optd_incremental_sql::store::PublishPoint;
use optd_incremental_sql::Error;
use tempfile::tempdir;

fn count_db_entries(root: &Path) -> (usize, usize) {
    let mut dirs = 0usize;
    let mut files = 0usize;
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if entry.file_type().unwrap().is_dir() && name.starts_with("db-") {
            dirs += 1;
            files += count_files_recursive(&entry.path());
        }
    }
    (dirs, files)
}

fn count_files_recursive(dir: &Path) -> usize {
    let mut total = 0usize;
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            total += count_files_recursive(&entry.path());
        } else {
            total += 1;
        }
    }
    total
}

#[tokio::test]
async fn compaction_reclaims_unreachable_history_while_preserving_reused_blocks_retries_and_recovery(
) {
    let dir = tempdir().unwrap();
    let opts = OpenOptions {
        checkpoint_interval: 4,
        incompatible_state: IncompatibleStatePolicy::Fail,
    };
    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    session
        .execute("CREATE TABLE items (id BIGINT, grp BIGINT, amt BIGINT)")
        .await
        .unwrap();

    // Insert 600 rows (3 separate 256-entry blocks so early untouched blocks stay anchored in db-2).
    let values = (1..=600_i64)
        .map(|i| format!("({i}, {}, {})", i % 5, i * 10))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(&format!("INSERT INTO items VALUES {values}"))
        .await
        .unwrap();

    session
        .execute(
            "CREATE MATERIALIZED VIEW mv_sum AS SELECT grp, SUM(amt) AS total FROM items GROUP BY grp",
        )
        .await
        .unwrap();
    session
        .execute("CREATE MATERIALIZED VIEW mv_cnt AS SELECT grp, COUNT(id) AS cnt FROM items GROUP BY grp")
        .await
        .unwrap();

    // Identified request at an early epoch (before the future compaction floor).
    let early_sql = "INSERT INTO items VALUES (601, 1, 1000)";
    let early_resp = session
        .execute_request("req-early", early_sql)
        .await
        .unwrap();
    assert!(!early_resp.replayed);
    let early_epoch = early_resp.epoch;

    // Issue 15 more single-row commits on the high end (touching only the tail block, leaving
    // early blocks reused from db-2, and rolling operator-state checkpoints forward every 4 commits).
    for i in 602..=616_i64 {
        session
            .execute(&format!(
                "INSERT INTO items VALUES ({i}, {}, {})",
                i % 5,
                i * 10
            ))
            .await
            .unwrap();
    }

    let (dirs_before, files_before) = count_db_entries(dir.path());
    assert!(
        dirs_before >= 18,
        "expected many epoch dirs before compact, got {dirs_before}"
    );

    let report = session
        .compact(RetentionPolicy { retain_epochs: 3 })
        .unwrap();
    assert_eq!(report.head_epoch, session.epoch());
    assert!(
        report.retained_from_epoch > early_epoch,
        "compaction should advance past early epoch {early_epoch}: {report:?}"
    );
    assert!(
        report.removed_dirs > 0 || report.removed_files > 0,
        "compaction should reclaim stale history: {report:?}"
    );
    assert!(report.reclaimed_bytes > 0, "{report:?}");

    let (dirs_after, files_after) = count_db_entries(dir.path());
    assert!(
        files_after < files_before,
        "files should decrease after compaction: {files_before} -> {files_after}"
    );
    assert!(
        dirs_after < dirs_before,
        "dirs should decrease after compaction: {dirs_before} -> {dirs_after}"
    );

    // 1. Identified retry from before the retained floor needs no history scan.
    let before_retry = session.store_work().unwrap();
    let retry_resp = session
        .execute_request("req-early", early_sql)
        .await
        .unwrap();
    assert!(retry_resp.replayed);
    assert_eq!(retry_resp.epoch, early_epoch);
    let retry_work = session.store_work().unwrap().since(&before_retry).unwrap();
    assert_eq!(retry_work.manifest_reads.files, 0);

    // 2. Output history verification over retained window succeeds.
    session.verify_output_history().unwrap();

    drop(session);

    // 3. Reopen incrementally (Fail policy) — proves all reused early blocks, operator state
    //    checkpoints and post-checkpoint deltas remain 100% intact!
    let mut reopened = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(reopened.recovery_work().restored_views, Some(2));
    assert_eq!(reopened.recovery_work().rebuilt_views, 0);

    // Verify full table and both MVs match independent full SQL.
    let actual_sum = reopened
        .execute("SELECT * FROM mv_sum ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    let expected_sum = reopened
        .execute("SELECT grp, SUM(amt) AS total FROM items GROUP BY grp ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual_sum, expected_sum);

    let actual_cnt = reopened
        .execute("SELECT * FROM mv_cnt ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    let expected_cnt = reopened
        .execute("SELECT grp, COUNT(id) AS cnt FROM items GROUP BY grp ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual_cnt, expected_cnt);

    // 4. Subsequent commits + second compaction + reopen keep working cleanly.
    reopened
        .execute("DELETE FROM items WHERE id = 1")
        .await
        .unwrap();
    reopened
        .execute("UPDATE items SET amt = 7777 WHERE id = 2")
        .await
        .unwrap();
    reopened.verify_output_history().unwrap();
    let final_sum = reopened
        .execute("SELECT * FROM mv_sum ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    let final_expected = reopened
        .execute("SELECT grp, SUM(amt) AS total FROM items GROUP BY grp ORDER BY grp")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(final_sum, final_expected);
}

#[tokio::test]
async fn compaction_checkpoints_a_changed_view_before_pruning_its_replay_history() {
    let dir = tempdir().unwrap();
    let opts = OpenOptions {
        checkpoint_interval: 16,
        incompatible_state: IncompatibleStatePolicy::Fail,
    };
    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    session
        .execute("CREATE TABLE t (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    session
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS s FROM t GROUP BY k")
        .await
        .unwrap();

    // 6 commits after checkpoint (all < K=16, so view still needs table_deltas since mv_checkpoint_epoch).
    for i in 1..=6_i64 {
        session
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i % 2, i * 10))
            .await
            .unwrap();
    }

    let head = session.epoch();
    let report = session
        .compact(RetentionPolicy { retain_epochs: 2 })
        .unwrap();
    assert_eq!(report.retained_from_epoch, head - 1);
    assert_eq!(report.retained_manifests, 2);
    assert_eq!(report.written_checkpoints, 1);
    assert_eq!(report.promoted_checkpoints, 0);
    assert!(report.checkpoint_bytes_written > 0);
    session.verify_output_history().unwrap();
    drop(session);

    let mut reopened = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(reopened.recovery_work().restored_views, Some(1));
    assert_eq!(reopened.recovery_work().rebuilt_views, 0);
    assert_eq!(reopened.recovery_work().replayed_epochs, Some(0));
    let actual = reopened
        .execute("SELECT * FROM mv ORDER BY k")
        .await
        .unwrap()
        .rows
        .unwrap();
    let reference = reopened
        .execute("SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual, reference);
}

#[tokio::test]
async fn idle_views_advance_without_state_writes_and_empty_output_deltas_do_not_promote() {
    let dir = tempdir().unwrap();
    let mut session = SqlSession::open(dir.path()).await.unwrap();
    session
        .execute_script(
            "CREATE TABLE idle(k BIGINT); CREATE TABLE busy(k BIGINT);
         CREATE MATERIALIZED VIEW stable AS SELECT COUNT(*) AS n FROM idle;
         CREATE MATERIALIZED VIEW filtered AS SELECT k FROM busy WHERE k < 0;",
        )
        .await
        .unwrap();
    // The input state of filtered changes, although its output stays empty.
    session
        .execute("INSERT INTO busy VALUES (1)")
        .await
        .unwrap();
    let head = session.epoch();
    let report = session
        .compact(RetentionPolicy { retain_epochs: 1 })
        .unwrap();
    assert_eq!(report.retained_from_epoch, head);
    assert_eq!(report.retained_manifests, 1);
    assert_eq!(report.promoted_checkpoints, 1);
    assert_eq!(report.written_checkpoints, 1);
    session.verify_output_history().unwrap();
    drop(session);

    let mut reopened = SqlSession::open(dir.path()).await.unwrap();
    assert_eq!(reopened.recovery_work().restored_views, Some(2));
    assert_eq!(reopened.recovery_work().replayed_epochs, Some(0));
    // Retraction needs the committed input state, even though its earlier insert
    // emitted no output rows.
    reopened
        .execute("DELETE FROM busy WHERE k = 1")
        .await
        .unwrap();
    let actual = reopened
        .execute("SELECT * FROM filtered")
        .await
        .unwrap()
        .rows
        .unwrap();
    let expected = reopened
        .execute("SELECT k FROM busy WHERE k < 0")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual, expected);
    // A subsequent compaction advances the still-idle view without rewriting it.
    let report = reopened
        .compact(RetentionPolicy { retain_epochs: 1 })
        .unwrap();
    assert_eq!(report.promoted_checkpoints, 1);
    reopened.verify_output_history().unwrap();
}

#[tokio::test]
async fn compaction_survives_publish_barriers_and_reclaims_orphan_crash_dirs() {
    let dir = tempdir().unwrap();
    let opts = OpenOptions {
        checkpoint_interval: 2,
        incompatible_state: IncompatibleStatePolicy::Fail,
    };
    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    session
        .execute("CREATE TABLE t (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    session
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) AS s FROM t GROUP BY k")
        .await
        .unwrap();
    for i in 1..=6_i64 {
        session
            .execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 5))
            .await
            .unwrap();
    }

    // Create a simulated orphan attempt directory left behind by an earlier crash.
    let orphan = dir.path().join("db-999-orphan-1");
    fs::create_dir(&orphan).unwrap();
    fs::write(orphan.join("stale.arrow"), b"unreferenced crash payload").unwrap();
    assert!(orphan.exists());

    // 1. Fail at FilesDurable during compaction -> CURRENT unchanged, nothing deleted, session fenced.
    let err = session
        .compact_with_observer(RetentionPolicy { retain_epochs: 2 }, &mut |point| {
            if point == PublishPoint::FilesDurable {
                Err(Error::Invalid("injected FilesDurable crash".into()))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(err.to_string().contains("FilesDurable"));
    assert!(
        orphan.exists(),
        "FilesDurable failure must not delete anything yet"
    );
    drop(session);

    // Reopen after FilesDurable failure: database is 100% intact.
    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(session.recovery_work().restored_views, Some(1));

    // 2. Fail at CurrentDurable during compaction -> CURRENT already published new floor, sweep
    //    did not run yet; reopening sees the valid compacted floor and next compact cleans orphans!
    let err = session
        .compact_with_observer(RetentionPolicy { retain_epochs: 2 }, &mut |point| {
            if point == PublishPoint::CurrentDurable {
                Err(Error::Invalid("injected CurrentDurable crash".into()))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(err.to_string().contains("CurrentDurable"));
    drop(session);

    let mut session = SqlSession::open_with_options(dir.path(), opts)
        .await
        .unwrap();
    assert_eq!(session.recovery_work().restored_views, Some(1));

    // 3. Complete compaction succeeds and removes the orphan directory plus stale epochs.
    let report = session
        .compact(RetentionPolicy { retain_epochs: 2 })
        .unwrap();
    assert!(
        !orphan.exists(),
        "compaction must clean orphan crash directories"
    );
    assert!(report.removed_dirs >= 1, "{report:?}");

    let actual = session
        .execute("SELECT * FROM mv ORDER BY k")
        .await
        .unwrap()
        .rows
        .unwrap();
    let expected = session
        .execute("SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k")
        .await
        .unwrap()
        .rows
        .unwrap();
    assert_eq!(actual, expected);
}
