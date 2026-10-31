//! Typed mark kernels and both physical orientations against independent bags.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{NullEquality, ScalarValue};
use datafusion::logical_expr::{col, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::super::KeyedState;
use crate::bag::Bag;
use crate::planner::plan::{IndexAlgorithm, IndexSpec, RelationOperator};
use crate::planner::KeyScope;
use crate::Result;

type Row = (Option<i64>, Option<i64>);

fn batch(rows: &[Row]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

/// Independent full-row bag: exactly one nonnullable flag per preserved row.
fn oracle(
    kind: JoinType,
    mode: usize,
    data: &[Vec<Row>; 2],
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<Bag> {
    let mut rows = HashMap::<Vec<ScalarValue>, i64>::new();
    let side = usize::from(kind == JoinType::RightMark);
    for own in &data[side] {
        let matched = data[1 - side].iter().any(|peer| {
            let (a, b) = if side == 0 { (own, peer) } else { (peer, own) };
            let key = if mode == 2 {
                a.0 == b.0
            } else {
                a.0.is_some() && a.0 == b.0
            };
            key && match mode {
                1 | 2 => matches!((a.1,b.1),(Some(a),Some(b)) if a<b),
                3 => a.1.is_some() && a.1 == b.1,
                _ => true,
            }
        });
        let row = vec![
            ScalarValue::Int64(own.0),
            ScalarValue::Int64(own.1),
            ScalarValue::Boolean(Some(matched)),
        ];
        *rows.entry(row).or_default() += 1;
    }
    Bag::from_unique_entries(schema, rows.iter().map(|(r, w)| (r.as_slice(), *w)))
}

async fn typed_plan(
    kind: JoinType,
    mode: usize,
    data: &[Vec<Row>; 2],
) -> Result<(SessionContext, LogicalPlan)> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    for (name, rows) in ["a", "b"].iter().zip(data) {
        ctx.register_batch(*name, batch(rows)?)?;
    }
    let left = ctx.table("a").await?.into_unoptimized_plan();
    let right = ctx.table("b").await?.into_unoptimized_plan();
    let keys = if mode == 3 {
        (vec!["a.k", "a.v"], vec!["b.k", "b.v"])
    } else {
        (vec!["a.k"], vec!["b.k"])
    };
    let LogicalPlan::Join(join) = LogicalPlanBuilder::from(left)
        .join(right, kind, keys, None)?
        .build()?
    else {
        panic!("typed join builder");
    };
    let filter = [1, 2].contains(&mode).then(|| col("a.v").lt(col("b.v")));
    let plan = LogicalPlan::Join(Join::try_new(
        join.left,
        join.right,
        join.on,
        filter,
        kind,
        join.join_constraint,
        if mode == 2 {
            NullEquality::NullEqualsNull
        } else {
            NullEquality::NullEqualsNothing
        },
        false,
    )?);
    Ok((ctx, plan))
}

async fn full_kernel(kind: JoinType, mode: usize, data: &[Vec<Row>; 2]) -> Result<Bag> {
    let (ctx, plan) = typed_plan(kind, mode, data).await?;
    let schema = Arc::new(plan.schema().as_arrow().clone());
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(2).name(), "mark");
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert!(!schema.field(2).is_nullable());
    let output = ctx.execute_logical_plan(plan).await?.collect().await?;
    let output = datafusion::arrow::compute::concat_batches(&schema, &output)?;
    let result = Bag::from_batch(&output)?;
    assert_eq!(result, oracle(kind, mode, data, schema)?);
    Ok(result)
}

#[tokio::test]
async fn both_typed_mark_kernels_and_signed_state_prefixes_match_independent_bags() -> Result<()> {
    for kind in [JoinType::LeftMark, JoinType::RightMark] {
        for mode in 0..4 {
            let mut data = [
                vec![(Some(1), Some(1)), (Some(1), Some(1)), (None, None)],
                vec![(Some(2), Some(2)), (None, None)],
            ];
            let (ctx, plan) = typed_plan(kind, mode, &data).await?;
            let operator = RelationOperator::from_logical(&plan)?;
            let columns = if mode == 3 { vec![0, 1] } else { vec![0] };
            let inputs = data.iter().map(|r| batch(r)).collect::<Result<Vec<_>>>()?;
            let keys = inputs
                .iter()
                .map(|b| Ok(b.project(&columns)?))
                .collect::<Result<Vec<_>>>()?;
            let spec = IndexSpec {
                id: 2,
                algorithm: IndexAlgorithm::MatchCounts(operator.clone()),
                scope: KeyScope::JoinKeys,
                inputs: vec![0, 1],
                projections: operator
                    .inputs
                    .iter()
                    .enumerate()
                    .map(|(side, schema)| {
                        RelationOperator::projection(
                            schema.clone(),
                            columns
                                .iter()
                                .map(|c| col(format!("{}.{}", ["a", "b"][side], ["k", "v"][*c])))
                                .collect(),
                        )
                    })
                    .collect::<Result<_>>()?,
                output_projection: None,
                before: true,
                after: true,
            };
            let mut state = KeyedState::new(&inputs, &keys, None)?;
            state.initialize_matches(&ctx, &spec)?;
            let mut replay = full_kernel(kind, mode, &data).await?;
            let mut seed = 0x8127_u64;
            for round in 0..48 {
                let mut positive = Vec::new();
                let mut negative = Vec::new();
                for rows in &mut data {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if !rows.is_empty() && (round % 10 > 5 || seed.is_multiple_of(3)) {
                        let row = rows.remove((seed as usize >> 5) % rows.len());
                        positive.push(batch(&[])?);
                        negative.push(batch(&[row])?);
                    } else {
                        let key = (seed >> 12) % 5;
                        let row = (
                            (key != 4).then_some(key as i64),
                            (round % 4 != 0).then_some((seed >> 20) as i64 % 4),
                        );
                        rows.push(row);
                        positive.push(batch(&[row])?);
                        negative.push(batch(&[])?);
                    }
                }
                let pkeys = positive
                    .iter()
                    .map(|b| Ok(b.project(&columns)?))
                    .collect::<Result<Vec<_>>>()?;
                let nkeys = negative
                    .iter()
                    .map(|b| Ok(b.project(&columns)?))
                    .collect::<Result<Vec<_>>>()?;
                let mut candidate = state.prepare(&positive, &pkeys, &negative, &nkeys, &spec)?;
                let (delta, _) = candidate.maintain_match_join(&operator)?;
                replay = replay.add(&delta)?;
                assert_eq!(
                    replay,
                    oracle(kind, mode, &data, operator.schema())?,
                    "{kind:?} mode {mode} round {round}"
                );
                if round % 12 == 0 {
                    assert_eq!(replay, full_kernel(kind, mode, &data).await?);
                }
                state = candidate.state;
            }
        }
    }
    Ok(())
}
