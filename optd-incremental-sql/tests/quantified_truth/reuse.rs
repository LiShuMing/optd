//! Identical observed membership shares typed operator states within one boundary.

use super::{batch, membership, tables, text, values, Rows};
use optd_incremental_sql::planner::{IncrementalOptimizer, Maintenance};
use optd_incremental_sql::{SqlView, TableChange};

#[tokio::test]
async fn repeated_in_and_not_in_share_one_typed_member_and_flag_binding() {
    for repeats in [2, 65] {
        let observations = (0..repeats)
            .map(|i| {
                format!(
                    "k {}IN (SELECT k FROM b) AS m{i}",
                    if i % 2 == 0 { "" } else { "NOT " }
                )
            })
            .collect::<Vec<_>>();
        let query = format!("SELECT k,{} FROM a", observations.join(","));
        let left = [Some(1), Some(2), None];
        let mut view = SqlView::new_with_optimizer(
            &query,
            tables(&left, &[Some(1)]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        let states = view
            .incremental_plan()
            .nodes()
            .iter()
            .filter(|n| {
                matches!(
                    n.maintenance,
                    Maintenance::Accumulate | Maintenance::ChangedGroups
                )
            })
            .count();
        assert_eq!(
            states, 2,
            "one distinct-member state and one exact count state"
        );
        assert_eq!(
            view.incremental_plan()
                .nodes()
                .iter()
                .filter(|n| matches!(n.maintenance, Maintenance::MatchJoin))
                .count(),
            1
        );
        for right in [&[Some(1)][..], &[Some(1), None][..]] {
            if right.len() == 2 {
                let candidate = view
                    .prepare(&[TableChange {
                        table: "b".into(),
                        rows: batch(&[None]),
                        weights: vec![1],
                    }])
                    .await
                    .unwrap();
                assert_eq!(candidate.delta().unwrap().num_rows(), 2);
                view.acknowledge(candidate).unwrap();
            }
            let mut expected = Rows::new();
            for key in left {
                let truth = membership(key, right);
                let mut row = vec![text(key)];
                for i in 0..repeats {
                    let value = if i % 2 == 0 { truth } else { truth.map(|v| !v) };
                    row.push(value.map_or("NULL".into(), |v| v.to_string()));
                }
                expected.insert(row, 1);
            }
            assert_eq!(values(&view.contents().unwrap(), false), expected);
        }
    }
}

#[tokio::test]
async fn distinct_comparisons_and_subqueries_never_share_truth() {
    let queries=["SELECT k,k IN (SELECT k FROM b) AS m,(k+1) IN (SELECT k FROM b) AS n FROM a","SELECT k,k IN (SELECT k FROM b WHERE k=1) AS m,k IN (SELECT k FROM b WHERE k=2) AS n FROM a"];
    for (mode, query) in queries.iter().enumerate() {
        let left = [Some(1), Some(2), None];
        let mut view = SqlView::new_with_optimizer(
            query,
            tables(&left, &[Some(1)]),
            IncrementalOptimizer::default().require_incremental(),
        )
        .await
        .unwrap();
        for right in [&[Some(1)][..], &[Some(1), Some(2), None][..]] {
            if right.len() == 3 {
                let candidate = view
                    .prepare(&[TableChange {
                        table: "b".into(),
                        rows: batch(&[Some(2), None]),
                        weights: vec![1, 1],
                    }])
                    .await
                    .unwrap();
                view.acknowledge(candidate).unwrap();
            }
            let mut expected = Rows::new();
            for key in left {
                let (m, n) = if mode == 0 {
                    (
                        membership(key, right),
                        membership(key.map(|v| v + 1), right),
                    )
                } else {
                    let first = right
                        .iter()
                        .copied()
                        .filter(|v| *v == Some(1))
                        .collect::<Vec<_>>();
                    let second = right
                        .iter()
                        .copied()
                        .filter(|v| *v == Some(2))
                        .collect::<Vec<_>>();
                    (membership(key, &first), membership(key, &second))
                };
                expected.insert(
                    vec![
                        text(key),
                        m.map_or("NULL".into(), |v| v.to_string()),
                        n.map_or("NULL".into(), |v| v.to_string()),
                    ],
                    1,
                );
            }
            assert_eq!(values(&view.contents().unwrap(), false), expected);
        }
    }
}
