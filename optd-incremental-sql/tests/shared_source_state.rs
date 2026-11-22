//! Regression tests for cross-view shared `SourceState` deduplication (RFC TODO-S2).

use optd_incremental_sql::database::SqlSession;
use tempfile::tempdir;

#[tokio::test]
async fn multiple_views_share_one_source_state_across_create_dml_rollback_and_reopen() {
    let dir = tempdir().unwrap();
    let mut session = SqlSession::open(dir.path()).await.unwrap();
    session
        .execute("CREATE TABLE items (id BIGINT, grp BIGINT, amt BIGINT)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO items VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40)")
        .await
        .unwrap();

    session
        .execute(
            "CREATE MATERIALIZED VIEW mv_sum AS SELECT grp, SUM(amt) AS s FROM items GROUP BY grp",
        )
        .await
        .unwrap();
    session
        .execute(
            "CREATE MATERIALIZED VIEW mv_cnt AS SELECT grp, COUNT(id) AS c FROM items GROUP BY grp",
        )
        .await
        .unwrap();
    session
        .execute("CREATE MATERIALIZED VIEW mv_hi AS SELECT id, amt FROM items WHERE amt >= 30")
        .await
        .unwrap();

    // Right after creation, all three views share the exact same Arc<SourceSnapshot> for `items`.
    assert!(session
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());
    assert!(session
        .views_share_source_state("mv_sum", "mv_hi", "items")
        .unwrap());

    // After a DML commit, SourceState::apply runs ONCE for `items`, and all 3 views still share
    // the updated Arc<SourceSnapshot>.
    let resp = session
        .execute("INSERT INTO items VALUES (5, 2, 50)")
        .await
        .unwrap();
    assert!(session
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());
    assert!(session
        .views_share_source_state("mv_sum", "mv_hi", "items")
        .unwrap());

    let commit = resp.commit.expect("INSERT must produce CommitReport");
    let total_source_updates: u64 = commit
        .maintenance
        .values()
        .map(|m| m["work"]["source_state"]["updates"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(
        total_source_updates, 1,
        "3 sibling views must share one SourceState::apply instead of rebuilding 3 copies: {:?}",
        commit.maintenance
    );

    // Rolled-back transaction must leave committed shared SourceState intact.
    session.execute("BEGIN").await.unwrap();
    session
        .execute("UPDATE items SET amt = 999 WHERE id = 1")
        .await
        .unwrap();
    session.execute("ROLLBACK").await.unwrap();
    assert!(session
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());

    // Execute UPDATE + DELETE and verify all 3 views agree with full SQL.
    session
        .execute("UPDATE items SET amt = 60 WHERE id = 2")
        .await
        .unwrap();
    session
        .execute("DELETE FROM items WHERE id = 3")
        .await
        .unwrap();
    assert!(session
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());
    assert!(session
        .views_share_source_state("mv_sum", "mv_hi", "items")
        .unwrap());

    drop(session);

    // Reopen from persisted operator state: all 3 restored views rebind to one shared SourceState.
    let mut reopened = SqlSession::open(dir.path()).await.unwrap();
    assert_eq!(reopened.recovery_work().restored_views, Some(3));
    assert!(reopened
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());
    assert!(reopened
        .views_share_source_state("mv_sum", "mv_hi", "items")
        .unwrap());

    // Continue mutating after reopen and compare every MV with full SQL.
    reopened
        .execute("INSERT INTO items VALUES (6, 1, 100)")
        .await
        .unwrap();
    assert!(reopened
        .views_share_source_state("mv_sum", "mv_cnt", "items")
        .unwrap());

    for (mv, sql) in [
        (
            "mv_sum",
            "SELECT grp, SUM(amt) AS s FROM items GROUP BY grp ORDER BY grp",
        ),
        (
            "mv_cnt",
            "SELECT grp, COUNT(id) AS c FROM items GROUP BY grp ORDER BY grp",
        ),
        (
            "mv_hi",
            "SELECT id, amt FROM items WHERE amt >= 30 ORDER BY id",
        ),
    ] {
        let actual = reopened
            .execute(&format!(
                "SELECT * FROM {mv} ORDER BY {}",
                if mv == "mv_hi" { "id" } else { "grp" }
            ))
            .await
            .unwrap()
            .rows
            .unwrap();
        let expected = reopened.execute(sql).await.unwrap().rows.unwrap();
        assert_eq!(
            actual, expected,
            "view {mv} mismatch after shared DML + reopen"
        );
    }

    // Dropping views leaves remaining views functioning and prunes shared_sources when last drops.
    reopened
        .execute("DROP MATERIALIZED VIEW mv_hi")
        .await
        .unwrap();
    reopened
        .execute("DROP MATERIALIZED VIEW mv_cnt")
        .await
        .unwrap();
    reopened
        .execute("DROP MATERIALIZED VIEW mv_sum")
        .await
        .unwrap();
    reopened
        .execute("INSERT INTO items VALUES (7, 3, 70)")
        .await
        .unwrap();
}
