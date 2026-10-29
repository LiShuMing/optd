//! Kill a real SQL refresh process at each durable publication boundary.
//!
//! Bootstrap is committed normally: DurableSqlView::open does not expose a
//! bootstrap observer. Successor publication is killed without Rust unwinding.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use optd_incremental_sql::store::{CheckpointStore, PublishPoint, Snapshot};
use optd_incremental_sql::{DurableSqlView, TableChange};

const SQL: &str = "SELECT a.k, SUM(a.v * b.v) AS total \
    FROM a JOIN b ON a.k=b.k GROUP BY a.k \
    ORDER BY total DESC, a.k ASC LIMIT 2";
type Rows = HashMap<Vec<ScalarValue>, i64>;
type SourceRows = [(i64, i64)];

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn expected_tables(epoch: u64) -> BTreeMap<String, RecordBatch> {
    let (a, b): (&SourceRows, &SourceRows) = match epoch {
        0 => (
            &[(1, 10), (1, 20), (2, 5), (3, 15)],
            &[(1, 2), (2, 1), (3, 1)],
        ),
        1 => (
            &[(1, 20), (2, 5), (3, 15), (4, 40)],
            &[(1, 2), (2, 1), (4, 3)],
        ),
        2 => (
            &[(1, 20), (2, 5), (3, 15), (2, 50)],
            &[(2, 1), (4, 3), (3, 2)],
        ),
        _ => panic!("test fixture has only epochs zero through two"),
    };
    BTreeMap::from([("a".into(), batch(a)), ("b".into(), batch(b))])
}

fn change(table: &str, rows: &[(i64, i64)], weights: &[i64]) -> TableChange {
    TableChange {
        table: table.into(),
        rows: batch(rows),
        weights: weights.to_vec(),
    }
}

fn first_changes() -> Vec<TableChange> {
    vec![
        change("a", &[(1, 10), (4, 40)], &[-1, 1]),
        change("b", &[(3, 1), (4, 3)], &[-1, 1]),
    ]
}

fn second_changes() -> Vec<TableChange> {
    vec![
        change("a", &[(4, 40), (2, 50)], &[-1, 1]),
        change("b", &[(1, 2), (3, 2)], &[-1, 1]),
    ]
}

fn scalar_row(batch: &RecordBatch, index: usize, columns: usize) -> Vec<ScalarValue> {
    batch.columns()[..columns]
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), index).unwrap())
        .collect()
}

fn add(rows: &mut Rows, key: Vec<ScalarValue>, weight: i64) {
    let next = rows
        .get(&key)
        .copied()
        .unwrap_or_default()
        .checked_add(weight)
        .unwrap();
    if next == 0 {
        rows.remove(&key);
    } else {
        rows.insert(key, next);
    }
}

fn bag(batch: &RecordBatch) -> Rows {
    let mut rows = Rows::new();
    for index in 0..batch.num_rows() {
        add(&mut rows, scalar_row(batch, index, batch.num_columns()), 1);
    }
    rows
}

fn assert_tables(actual: &BTreeMap<String, RecordBatch>, epoch: u64) {
    let expected = expected_tables(epoch);
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (name, expected) in expected {
        assert_eq!(
            actual[&name].schema(),
            expected.schema(),
            "source schema {name}"
        );
        assert_eq!(
            bag(&actual[&name]),
            bag(&expected),
            "source rows {name} at epoch {epoch}"
        );
    }
}

async fn oracle(epoch: u64) -> RecordBatch {
    // A fresh full SQL context receives hand-specified sources, not recovered
    // engine state, so a shared source-application error cannot validate itself.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, batch) in expected_tables(epoch) {
        ctx.register_batch(name.as_str(), batch).unwrap();
    }
    let frame = ctx.sql(SQL).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    concat_batches(&schema, &frame.collect().await.unwrap()).unwrap()
}

async fn assert_view(view: &DurableSqlView, epoch: u64) {
    assert_eq!(view.view().epoch(), epoch);
    assert_tables(&view.view().tables().unwrap(), epoch);
    // Exact batch equality also validates ORDER BY / LIMIT presentation.
    assert_eq!(view.view().contents().unwrap(), oracle(epoch).await);
}

async fn assert_history(history: &[Snapshot], through: u64) {
    assert_eq!(history.len(), usize::try_from(through + 1).unwrap());
    let mut replayed = Rows::new();
    for (index, snapshot) in history.iter().enumerate() {
        let epoch = u64::try_from(index).unwrap();
        assert_eq!(snapshot.epoch, epoch);
        assert_eq!(snapshot.sql, SQL);
        assert_tables(&snapshot.tables, epoch);
        assert_eq!(snapshot.output, oracle(epoch).await);
        let payload_columns = snapshot.output.num_columns();
        assert_eq!(snapshot.delta.num_columns(), payload_columns + 1);
        assert_eq!(
            snapshot.delta.schema().fields()[..payload_columns],
            snapshot.output.schema().fields()[..]
        );
        assert_eq!(
            snapshot.delta.schema().field(payload_columns).name(),
            "__optd_weight"
        );
        for row in 0..snapshot.delta.num_rows() {
            let weight =
                ScalarValue::try_from_array(snapshot.delta.column(payload_columns).as_ref(), row)
                    .unwrap();
            let ScalarValue::Int64(Some(weight)) = weight else {
                panic!("nonnull signed Int64 weight required");
            };
            assert_ne!(weight, 0);
            add(
                &mut replayed,
                scalar_row(&snapshot.delta, row, payload_columns),
                weight,
            );
        }
        assert!(replayed.values().all(|weight| *weight > 0));
        assert_eq!(
            replayed,
            bag(&snapshot.output),
            "independent log replay at epoch {epoch}"
        );
    }
}

/// Own the exact spawned process even if an assertion or file operation fails.
struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn kill_refresh_at(root: &Path, point: &str) {
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_refresh_subprocess_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("OPTD_SQL_ENGINE_KILL_ROOT", root)
            .env("OPTD_SQL_ENGINE_KILL_POINT", point)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let marker = root.join("sql-barrier");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        if let Some(status) = child.0.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(pipe) = child.0.stderr.take() {
                pipe.take(64 * 1024).read_to_string(&mut stderr).unwrap();
            }
            panic!("SQL worker exited before {point} barrier ({status}): {stderr}");
        }
        assert!(
            Instant::now() < deadline,
            "SQL worker failed to reach {point} barrier within 30 seconds"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Verify the marker belongs to this child and exact target epoch/window.
    assert_eq!(
        fs::read_to_string(marker).unwrap(),
        format!("{}:{point}:1", child.0.id())
    );
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(9),
            "must be a real SIGKILL, not a callback error/unwind"
        );
    }
}

#[test]
fn sql_refresh_subprocess_worker() {
    let Ok(root) = std::env::var("OPTD_SQL_ENGINE_KILL_ROOT") else {
        return;
    };
    let point = std::env::var("OPTD_SQL_ENGINE_KILL_POINT").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut view = DurableSqlView::open(&root, SQL, BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(view.view().epoch(), 0);
        view.refresh_with_observer(&first_changes(), |observed| {
            let matches = match observed {
                PublishPoint::FilesDurable => point == "files",
                PublishPoint::CurrentDurable => point == "current",
            };
            if matches {
                let directory = Path::new(&root);
                fs::write(
                    directory.join("sql-barrier.tmp"),
                    format!("{}:{point}:1", std::process::id()),
                )
                .unwrap();
                fs::rename(
                    directory.join("sql-barrier.tmp"),
                    directory.join("sql-barrier"),
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
    panic!("worker failed to stop at the requested publication boundary");
}

#[tokio::test]
async fn real_sql_process_death_recovers_sources_ordered_topk_and_output_log() {
    for point in ["files", "current"] {
        let root = tempfile::tempdir().unwrap();
        let bootstrap = DurableSqlView::open(root.path(), SQL, expected_tables(0))
            .await
            .unwrap();
        assert_view(&bootstrap, 0).await;
        drop(bootstrap);
        let original_current = fs::read(root.path().join("CURRENT")).unwrap();
        kill_refresh_at(root.path(), point);

        let committed = if point == "files" { 0 } else { 1 };
        if committed == 0 {
            assert_eq!(
                fs::read(root.path().join("CURRENT")).unwrap(),
                original_current
            );
        } else {
            assert_ne!(
                fs::read(root.path().join("CURRENT")).unwrap(),
                original_current
            );
        }
        let store = CheckpointStore::open(root.path()).unwrap();
        let snapshot = store.load().unwrap().unwrap();
        assert_eq!(snapshot.epoch, committed);
        assert_history(&store.history().unwrap(), committed).await;
        drop(store);

        let mut recovered = DurableSqlView::open(root.path(), SQL, BTreeMap::new())
            .await
            .unwrap();
        assert_view(&recovered, committed).await;
        if point == "files" {
            // The source batch was not committed; retry it exactly once.
            recovered.refresh(&first_changes()).await.unwrap();
            assert_view(&recovered, 1).await;
        }
        // In the CurrentDurable window the first batch already committed. Both
        // paths now submit a genuinely new second batch, not a duplicate replay.
        recovered.refresh(&second_changes()).await.unwrap();
        assert_view(&recovered, 2).await;
        drop(recovered);
        let reopened = DurableSqlView::open(root.path(), SQL, BTreeMap::new())
            .await
            .unwrap();
        assert_view(&reopened, 2).await;
        drop(reopened);
        let store = CheckpointStore::open(root.path()).unwrap();
        assert_history(&store.history().unwrap(), 2).await;
    }
}
