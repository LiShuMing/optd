//! Owned, epoch-bound scan position; no directory walk at construction or producer task.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use super::RowBlocks;
use crate::error::invalid;
use crate::{Error, Result, RowReadWork};

/// Position retains one boundary key, independent of the number of stored leaves.
enum Position {
    Start,
    At { key: Vec<u8>, offset: usize },
    End,
}

/// Independent consumer of one immutable row revision and projected schema.
pub(crate) struct RowCursor {
    rows: RowBlocks,
    projection: Vec<usize>,
    position: Position,
}

impl RowCursor {
    /// Bind metadata only; projection validation cannot demand a leaf cache.
    pub(crate) fn new(rows: RowBlocks, projection: Vec<usize>) -> Result<Self> {
        rows.schema().project(&projection)?;
        Ok(Self {
            rows,
            projection,
            position: Position::Start,
        })
    }

    /// Demand at most one bounded projected chunk. EOF and errors permanently stop this cursor.
    pub(crate) fn next(
        &mut self,
        bound: usize,
        work: &mut RowReadWork,
    ) -> Result<Option<RecordBatch>> {
        if bound == 0 {
            self.position = Position::End;
            return Err(invalid("zero row scan batch bound"));
        }
        let position = std::mem::replace(&mut self.position, Position::End);
        let blocks = &self.rows.0.blocks;
        let (key, block, offset) = match position {
            Position::Start => match blocks.first_key_value() {
                Some((key, block)) => (key.clone(), Arc::clone(block), 0),
                None => return Ok(None),
            },
            Position::At { key, offset } => {
                let block = blocks
                    .get(&key)
                    .ok_or_else(|| invalid("scan leaf absent in immutable revision"))?;
                if offset < block.row_count {
                    (key, Arc::clone(block), offset)
                } else {
                    match blocks.successor(&key) {
                        Some((key, block)) => (key.clone(), Arc::clone(block), 0),
                        None => return Ok(None),
                    }
                }
            }
            Position::End => return Ok(None),
        };
        let count = bound.min(block.row_count - offset);
        work.leaf_chunks = work.leaf_chunks.checked_add(1).ok_or(Error::Overflow)?;
        // Guard only the cache decision/conversion; no asynchronous operation holds it.
        let mut cache = block
            .batch
            .lock()
            .map_err(|_| invalid("row leaf cache poisoned"))?;
        let batch = if let Some(batch) = cache.as_ref() {
            work.cache_hits = work.cache_hits.checked_add(1).ok_or(Error::Overflow)?;
            batch.slice(offset, count).project(&self.projection)?
        } else {
            let batch = block
                .rows
                .range_batch(offset, count, &self.projection, work)?;
            if offset == 0
                && count == block.row_count
                && self
                    .projection
                    .iter()
                    .copied()
                    .eq(0..self.rows.num_columns())
            {
                *cache = Some(batch.clone());
            }
            batch
        };
        self.position = Position::At {
            key,
            offset: offset + count,
        };
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bag::Bag;
    use crate::row_blocks::LeafCachePolicy;
    use datafusion::arrow::array::{
        ArrayRef, DictionaryArray, Int16Array, Int64Array, ListArray, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::compute::{concat_batches, take};
    use datafusion::arrow::datatypes::{DataType, Field, Int16Type, Int64Type, Schema};

    fn ints(keys: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(keys))],
        )
        .unwrap()
    }

    #[test]
    fn sql_target_writes_validate_distinct_rows_without_weight_expansion() -> Result<()> {
        let bootstrap = RowBlocks::from_batch(ints(vec![Some(1); 100_000]))?;
        assert_eq!(bootstrap.work.materialized_rows, 0);
        assert_eq!(bootstrap.leaf_validation.rows, 1);
        let updated = bootstrap
            .rows
            .apply(&Bag::from_batch(&ints(vec![Some(1)]))?)?;
        assert_eq!(updated.work.delta_update.materialized_rows, 0);
        assert_eq!(updated.work.leaf_validation.rows, 1);
        assert_eq!(updated.rows.num_rows(), 100_001);
        assert!(updated
            .rows
            .0
            .blocks
            .first_key_value()
            .unwrap()
            .1
            .batch
            .lock()
            .unwrap()
            .is_none());
        Ok(())
    }

    #[test]
    fn random_ranges_and_metadata_match_independent_integer_vectors() -> Result<()> {
        let input = (0..600).map(Some).collect::<Vec<_>>();
        let mut rows = RowBlocks::from_relation_batch(ints(input.clone()))?.rows;
        let original = rows.clone();
        let mut oracle = input;
        for step in 0..80 {
            let value = if step % 5 == 0 {
                None
            } else {
                Some((step * 31 % 600) as i64)
            };
            let weight = if step % 2 == 0 { 3 } else { 1 };
            rows = rows
                .apply(&Bag::from_weighted(&ints(vec![value]), &[weight])?)?
                .rows;
            oracle.extend(std::iter::repeat_n(value, weight as usize));
            assert_eq!(
                rows.null_counts(),
                &[oracle.iter().filter(|v| v.is_none()).count()]
            );
            if step % 9 == 0 {
                let mut sorted = oracle.clone();
                sorted.sort();
                let mut cursor = RowCursor::new(rows.clone(), vec![0])?;
                let mut batches = Vec::new();
                let mut work = crate::RowReadWork::default();
                while let Some(batch) = cursor.next(17, &mut work)? {
                    assert!(batch.num_rows() <= 17);
                    batches.push(batch);
                }
                assert_eq!(concat_batches(&rows.schema(), &batches)?, ints(sorted));
                assert!(cursor.next(17, &mut work)?.is_none());
            }
        }
        assert_eq!(original.null_counts(), &[0]);
        assert_eq!(original.num_rows(), 600);
        let mut zero = RowCursor::new(rows.clone(), vec![])?;
        let batch = zero.next(13, &mut crate::RowReadWork::default())?.unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 13);
        assert!(RowCursor::new(rows.clone(), vec![1]).is_err());
        assert!(zero.next(0, &mut crate::RowReadWork::default()).is_err());
        let leaf = rows.0.blocks.first_key_value().unwrap().1;
        assert!(leaf
            .rows
            .range_batch(usize::MAX, 1, &[0], &mut crate::RowReadWork::default())
            .is_err());
        assert!(leaf
            .rows
            .range_batch(
                0,
                leaf.row_count + 1,
                &[0],
                &mut crate::RowReadWork::default()
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn logical_null_metadata_matches_independent_arrow_dictionary_and_nested_arrays() -> Result<()>
    {
        let fields = vec![Arc::new(Field::new("child", DataType::Int64, true))].into();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(DictionaryArray::<Int16Type>::try_new(
                Int16Array::from(vec![Some(0), Some(1), None, Some(1)]),
                Arc::new(StringArray::from(vec![None, Some("v")])),
            )?),
            Arc::new(StructArray::new(
                fields,
                vec![Arc::new(Int64Array::from(vec![
                    None,
                    Some(1),
                    Some(2),
                    None,
                ]))],
                Some(NullBuffer::from(vec![true, false, true, true])),
            )),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![None, Some(1)]),
                None,
                Some(vec![]),
                Some(vec![Some(2)]),
            ])),
        ];
        // Qualify each admitted type independently, including duplicate top-level NULLs.
        for array in arrays {
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "v",
                    array.data_type().clone(),
                    true,
                )])),
                vec![Arc::clone(&array)],
            )?;
            for policy in [LeafCachePolicy::OnDemand, LeafCachePolicy::OnUpdate] {
                let original = RowBlocks::bootstrap(input.clone(), policy)?.rows;
                assert_eq!(original.null_counts(), &[array.logical_null_count()]);
                let repeated = original
                    .apply(&Bag::from_weighted(&input.slice(0, 1), &[5])?)?
                    .rows;
                let added_nulls = array.slice(0, 1).logical_null_count() * 5;
                assert_eq!(
                    repeated.null_counts(),
                    &[array.logical_null_count() + added_nulls]
                );
                let changed = repeated
                    .apply(&Bag::from_weighted(&input.slice(0, 1), &[-6])?)?
                    .rows;
                let kept = take(
                    array.as_ref(),
                    &datafusion::arrow::array::UInt32Array::from(vec![1, 2, 3]),
                    None,
                )?;
                assert_eq!(changed.null_counts(), &[kept.logical_null_count()]);
                assert_eq!(original.null_counts(), &[array.logical_null_count()]);
            }
        }
        Ok(())
    }

    #[test]
    fn complete_leaf_cache_is_reused_but_partial_and_projected_chunks_do_not_populate_it(
    ) -> Result<()> {
        let rows = RowBlocks::from_relation_batch(ints(vec![Some(1); 100_000]))?.rows;
        let mut work = crate::RowReadWork::default();
        for _ in 0..2 {
            let mut cursor = RowCursor::new(rows.clone(), vec![0])?;
            assert_eq!(cursor.next(1, &mut work)?.unwrap().num_rows(), 1);
        }
        assert_eq!(work.materialization.materialized_rows, 2);
        assert_eq!(work.cache_hits, 0);
        assert_eq!(work.stored_entries_visited, 2);
        assert!(rows
            .0
            .blocks
            .first_key_value()
            .unwrap()
            .1
            .batch
            .lock()
            .unwrap()
            .is_none());
        let mut cursor = RowCursor::new(rows.clone(), vec![0])?;
        assert_eq!(
            cursor.next(100_000, &mut work)?.unwrap().num_rows(),
            100_000
        );
        let before = work.clone();
        let mut cursor = RowCursor::new(rows, vec![])?;
        assert_eq!(cursor.next(1, &mut work)?.unwrap().num_columns(), 0);
        assert_eq!(work.materialization, before.materialization);
        assert_eq!(work.cache_hits, 1);
        Ok(())
    }
}
