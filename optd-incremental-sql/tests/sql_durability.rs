//! SQL-level recovery must validate the query, not only the checkpoint bytes.
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use optd_incremental_sql::store::PublishPoint;
use optd_incremental_sql::{DurableSqlView, Error, TableChange};

fn batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

#[tokio::test]
async fn committed_but_unacknowledged_multi_table_sql_resumes_exact_epoch() {
    let root = tempfile::tempdir().unwrap();
    let sql = "SELECT a.id, count(*) AS n FROM a JOIN b ON a.id=b.id GROUP BY a.id ORDER BY a.id";
    let inputs = BTreeMap::from([
        ("a".into(), batch(&[1, 1, 2])),
        ("b".into(), batch(&[1, 2])),
    ]);
    let mut view = DurableSqlView::open(root.path(), sql, inputs)
        .await
        .unwrap();
    let changes = vec![
        TableChange {
            table: "a".into(),
            rows: batch(&[1, 3]),
            weights: vec![-1, 1],
        },
        TableChange {
            table: "b".into(),
            rows: batch(&[3]),
            weights: vec![1],
        },
    ];
    let error = view
        .refresh_with_observer(&changes, |point| {
            if point == PublishPoint::CurrentDurable {
                Err(Error::Invalid("lost response".into()))
            } else {
                Ok(())
            }
        })
        .await;
    assert!(error.is_err());
    assert_eq!(view.view().epoch(), 0);
    assert!(
        view.refresh(&changes).await.is_err(),
        "must not replay onto a committed epoch"
    );
    drop(view);
    let mut recovered = DurableSqlView::open(root.path(), sql, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(recovered.view().epoch(), 1);
    assert_eq!(recovered.view().contents().unwrap().num_rows(), 3);
    assert_eq!(recovered.view().tables().unwrap()["a"].num_rows(), 3);
    recovered.refresh(&[]).await.unwrap();
    assert_eq!(recovered.view().epoch(), 2);
}

#[tokio::test]
async fn unpublished_files_and_failed_prepare_preserve_prior_sql_state() {
    let root = tempfile::tempdir().unwrap();
    let sql = "SELECT id FROM a WHERE id > 0 ORDER BY id LIMIT 2";
    let mut view = DurableSqlView::open(
        root.path(),
        sql,
        BTreeMap::from([("a".into(), batch(&[1, 3]))]),
    )
    .await
    .unwrap();
    let changes = vec![TableChange {
        table: "a".into(),
        rows: batch(&[2]),
        weights: vec![1],
    }];
    assert!(view
        .refresh_with_observer(&changes, |point| {
            if point == PublishPoint::FilesDurable {
                Err(Error::Invalid("before publication".into()))
            } else {
                Ok(())
            }
        })
        .await
        .is_err());
    assert_eq!(view.view().epoch(), 0);
    drop(view);
    let mut recovered = DurableSqlView::open(root.path(), sql, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(recovered.view().epoch(), 0);
    recovered.refresh(&changes).await.unwrap();
    assert_eq!(recovered.view().epoch(), 1);
    let before = recovered.view().contents().unwrap();
    let invalid = vec![TableChange {
        table: "a".into(),
        rows: batch(&[999]),
        weights: vec![-1],
    }];
    assert!(recovered.refresh(&invalid).await.is_err());
    assert_eq!(recovered.view().contents().unwrap(), before);
    assert_eq!(recovered.view().epoch(), 1);
    drop(recovered);
    assert!(
        DurableSqlView::open(root.path(), "SELECT * FROM a", BTreeMap::new())
            .await
            .is_err()
    );
}
