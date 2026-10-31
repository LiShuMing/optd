//! Unchanged state keys must not be copied into isolated refresh candidates.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use optd_incremental_sql::planner::IncrementalOptimizer;
use optd_incremental_sql::{SqlView, TableChange};

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn multiset(batch: &RecordBatch) -> HashMap<Vec<ScalarValue>, i64> {
    let mut rows = HashMap::new();
    for i in 0..batch.num_rows() {
        let row = batch
            .columns()
            .iter()
            .map(|a| ScalarValue::try_from_array(a, i).unwrap())
            .collect();
        *rows.entry(row).or_default() += 1;
    }
    rows
}

#[tokio::test]
async fn fixed_affected_key_copies_only_balanced_paths_across_all_state_families() {
    let queries = [
        "SELECT k,SUM(v) AS s FROM t GROUP BY k",
        "SELECT k,MIN(v) AS m FROM t GROUP BY k",
        "SELECT t.k,t.v,u.v AS uv FROM t LEFT JOIN u ON t.k=u.k",
        "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v) AS rn FROM t",
    ];
    for n in [256usize, 2048] {
        let original = (0..n as i64).map(|k| (k, k * 10)).collect::<Vec<_>>();
        for sql in queries {
            let tables = BTreeMap::from([
                ("t".into(), batch(&original)),
                ("u".into(), batch(&original)),
            ]);
            let view = SqlView::new_with_optimizer(
                sql,
                tables,
                IncrementalOptimizer::default().require_incremental(),
            )
            .await
            .unwrap();
            let old_output = multiset(&view.contents().unwrap());
            let candidate = view
                .prepare(&[TableChange {
                    table: "t".into(),
                    rows: batch(&[(0, 3)]),
                    weights: vec![1],
                }])
                .await
                .unwrap();
            let work = &candidate.stats().work.state_copies;
            eprintln!("directory-work n={n} sql={sql} {work:?}");
            let bound = 4 * (n.ilog2() as usize + 1);
            for count in [
                work.group_entries,
                work.accumulator_entries,
                work.keyed_input_entries,
                work.window_output_entries,
            ] {
                assert!(count <= bound, "{sql}: n={n}, {work:?}, bound={bound}");
            }
            assert!(work.group_entries + work.accumulator_entries + work.keyed_input_entries > 0);
            // The oracle has independently constructed full table endpoints.
            let ctx = SessionContext::new();
            let mut expected = original.clone();
            expected.push((0, 3));
            ctx.register_batch("t", batch(&expected)).unwrap();
            ctx.register_batch("u", batch(&original)).unwrap();
            let frame = ctx.sql(sql).await.unwrap();
            let schema = Arc::new(frame.schema().as_arrow().clone());
            let full = concat_batches(&schema, &frame.collect().await.unwrap()).unwrap();
            assert_eq!(multiset(candidate.contents()), multiset(&full));
            assert_eq!(multiset(&view.contents().unwrap()), old_output);
            drop(candidate);
            for changes in [
                vec![],
                vec![TableChange {
                    table: "t".into(),
                    rows: batch(&[(0, 3), (0, 3)]),
                    weights: vec![1, -1],
                }],
            ] {
                let no_op = view.prepare(&changes).await.unwrap();
                assert_eq!(no_op.stats().work.state_copies, Default::default());
                assert_eq!(multiset(no_op.contents()), old_output);
            }
        }
    }
}
