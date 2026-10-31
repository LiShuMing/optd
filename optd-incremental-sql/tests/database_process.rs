//! Real process death must never publish only a subset of a SQL transaction.
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use optd_incremental_sql::database::{display_rows, SqlSession};
use optd_incremental_sql::store::PublishPoint;

const WRITE: &str = "UPDATE a SET k=2,v=20 WHERE k=1; DELETE FROM b; INSERT INTO b VALUES (2,3)";
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn kill_at(root: &Path, point: &str, epoch: u64) {
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "database_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("OPTD_DATABASE_KILL_ROOT", root)
            .env("OPTD_DATABASE_KILL_POINT", point)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let marker = root.join("barrier");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        if let Some(status) = child.0.try_wait().unwrap() {
            let mut error = String::new();
            if let Some(pipe) = child.0.stderr.take() {
                pipe.take(65536).read_to_string(&mut error).unwrap();
            }
            panic!("worker exited before {point}: {status}: {error}");
        }
        assert!(Instant::now() < deadline, "worker deadline at {point}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        fs::read_to_string(marker).unwrap(),
        format!("{}:{point}:{epoch}", child.0.id())
    );
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
}

#[test]
fn database_worker() {
    let Ok(root) = std::env::var("OPTD_DATABASE_KILL_ROOT") else {
        return;
    };
    let point = std::env::var("OPTD_DATABASE_KILL_POINT").unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut db = SqlSession::open(&root).await.unwrap();
            let epoch = db.epoch() + 1;
            db.execute_transaction_observed("atomic-write", WRITE, |p| {
                if matches!(
                    (p, point.as_str()),
                    (PublishPoint::FilesDurable, "files")
                        | (PublishPoint::CurrentDurable, "current")
                ) {
                    fs::write(
                        Path::new(&root).join("barrier.tmp"),
                        format!("{}:{point}:{epoch}", std::process::id()),
                    )
                    .unwrap();
                    fs::rename(
                        Path::new(&root).join("barrier.tmp"),
                        Path::new(&root).join("barrier"),
                    )
                    .unwrap();
                    loop {
                        std::thread::park_timeout(Duration::from_millis(100));
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        });
    panic!("worker missed publication barrier");
}

async fn assert_contents(db: &mut SqlSession, new: bool) {
    for (query, expected) in if new {
        [
            ("SELECT * FROM a", vec![vec!["2", "20"]]),
            ("SELECT * FROM b", vec![vec!["2", "3"]]),
            ("SELECT * FROM joined", vec![vec!["2", "60"]]),
            ("SELECT * FROM grouped", vec![vec!["2", "20"]]),
        ]
    } else {
        [
            ("SELECT * FROM a", vec![vec!["1", "10"]]),
            ("SELECT * FROM b", vec![vec!["1", "2"]]),
            ("SELECT * FROM joined", vec![vec!["1", "20"]]),
            ("SELECT * FROM grouped", vec![vec!["1", "10"]]),
        ]
    } {
        let result = db.execute(query).await.unwrap().rows.unwrap();
        assert_eq!(display_rows(&result).unwrap(), expected, "{query}");
    }
    db.verify_output_history().unwrap();
}

#[tokio::test]
async fn sigkill_recovers_all_tables_views_and_identified_transaction_exactly_once() {
    for point in ["files", "current"] {
        let root = tempfile::tempdir().unwrap();
        let mut db = SqlSession::open(root.path()).await.unwrap();
        db.execute_script("CREATE TABLE a(k BIGINT,v BIGINT); CREATE TABLE b(k BIGINT,v BIGINT); INSERT INTO a VALUES (1,10); INSERT INTO b VALUES (1,2); CREATE MATERIALIZED VIEW joined AS SELECT a.k,a.v*b.v AS total FROM a JOIN b ON a.k=b.k; CREATE MATERIALIZED VIEW grouped AS SELECT k,SUM(v) AS total FROM a GROUP BY k").await.unwrap();
        let epoch = db.epoch();
        drop(db);
        let before = fs::read(root.path().join("CURRENT")).unwrap();
        kill_at(root.path(), point, epoch + 1);
        let mut recovered = SqlSession::open(root.path()).await.unwrap();
        let published = point == "current";
        assert_eq!(recovered.epoch(), epoch + u64::from(published));
        assert_eq!(
            fs::read(root.path().join("CURRENT")).unwrap() == before,
            !published
        );
        assert_contents(&mut recovered, published).await;
        let retry = recovered
            .execute_transaction("atomic-write", WRITE)
            .await
            .unwrap();
        assert_eq!(retry.replayed, published);
        assert_eq!(retry.epoch, epoch + 1);
        assert_contents(&mut recovered, true).await;
        assert!(
            recovered
                .execute_transaction("atomic-write", WRITE)
                .await
                .unwrap()
                .replayed
        );
        drop(recovered);
        assert_contents(&mut SqlSession::open(root.path()).await.unwrap(), true).await;
    }
}
