//! Immutable ordered table leaves; only touched leaves are rebuilt by a statement.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;

use super::dml_work::{DmlTargetWork, TableBlockWork};
use super::store_work::{TableBootstrapWork, TableMaterializationWork};
use crate::bag::{self, Bag};
use crate::error::invalid;
use crate::state_directory::StateDirectory;
use crate::{Error, Result};

/// Bound historical distinct-entry copying; expanded duplicate rows may be larger.
const BLOCK_ENTRIES: usize = 256;

/// Unsupported row encoders retain Bag's existing singleton admission behavior.
#[derive(Clone)]
enum RowOrder {
    Encoded(Arc<RowConverter>),
    SingleLeaf,
}

/// Shared positive row state and its already ordered scan batch.
struct TableBlock {
    rows: Bag,
    batch: RecordBatch,
    scalar_bytes: usize,
}

/// Contents of one immutable table revision, shared across catalog clones.
struct TableSnapshot {
    schema: SchemaRef,
    blocks: StateDirectory<Arc<TableBlock>>,
    row_count: usize,
    scalar_bytes: usize,
    order: RowOrder,
    // Flatten only at consumers that require one batch. No lock crosses await.
    flat: Mutex<Option<RecordBatch>>,
}

/// Clone a table revision by sharing its directory, leaves and flattened cache.
#[derive(Clone)]
pub(super) struct TableRows(Arc<TableSnapshot>);

/// Actual single-batch cache result, with a measured miss rather than inferred work.
pub(super) struct TableMaterialization {
    pub(super) batch: RecordBatch,
    pub(super) work: TableMaterializationWork,
}

/// Imported leaves and the actual conversion work used to build them.
pub(super) struct TableBootstrap {
    pub(super) rows: TableRows,
    pub(super) work: TableBootstrapWork,
}

/// A candidate table revision and its statement-local measured update work.
pub(super) struct TableUpdate {
    pub(super) rows: TableRows,
    pub(super) work: DmlTargetWork,
}

impl TableRows {
    /// Bootstrap once from an imported/created batch; later statements use leaves.
    pub(super) fn from_batch(batch: RecordBatch) -> Result<TableBootstrap> {
        let rows = Bag::from_batch(&batch)?;
        let volume = rows.materialization_volume()?;
        let mut work = TableBootstrapWork {
            collected_rows: batch.num_rows() as u64,
            distinct_entries: rows.len() as u64,
            ..Default::default()
        };
        let order = if batch.schema().fields().is_empty() {
            RowOrder::SingleLeaf
        } else {
            match RowConverter::new(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| SortField::new(f.data_type().clone()))
                    .collect(),
            ) {
                Ok(converter) => RowOrder::Encoded(Arc::new(converter)),
                Err(_) => RowOrder::SingleLeaf,
            }
        };
        let mut snapshot = TableSnapshot {
            schema: batch.schema(),
            blocks: StateDirectory::new(),
            row_count: volume.rows,
            scalar_bytes: volume.scalar_bytes,
            order,
            flat: Mutex::new(Some(batch)),
        };
        for leaf in rows.split(BLOCK_ENTRIES)? {
            let (key, block) = make_block(leaf, &snapshot.order)?;
            work.leaf_materializations = work
                .leaf_materializations
                .checked_add(1)
                .ok_or(Error::Overflow)?;
            work.materialized_rows = work
                .materialized_rows
                .checked_add(block.batch.num_rows() as u64)
                .ok_or(Error::Overflow)?;
            work.materialized_array_bytes = work
                .materialized_array_bytes
                .checked_add(block.batch.get_array_memory_size() as u64)
                .ok_or(Error::Overflow)?;
            snapshot.blocks.insert(key, Arc::new(block));
        }
        Ok(TableBootstrap {
            rows: Self(Arc::new(snapshot)),
            work,
        })
    }

    pub(super) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0.schema)
    }
    pub(super) fn num_rows(&self) -> usize {
        self.0.row_count
    }
    pub(super) fn num_columns(&self) -> usize {
        self.0.schema.fields().len()
    }

    /// MemTable consumes shared ordered leaves, without concatenating the table.
    pub(super) fn batches(&self) -> Vec<RecordBatch> {
        if self.0.blocks.is_empty() {
            return vec![RecordBatch::new_empty(self.schema())];
        }
        self.0.blocks.values().map(|b| b.batch.clone()).collect()
    }

    /// Cache one concatenation per immutable revision, including empty revisions.
    pub(super) fn materialize(&self) -> Result<TableMaterialization> {
        let mut cache = self
            .0
            .flat
            .lock()
            .map_err(|_| invalid("table flat cache poisoned"))?;
        if let Some(batch) = cache.as_ref() {
            return Ok(TableMaterialization {
                batch: batch.clone(),
                work: TableMaterializationWork::default(),
            });
        }
        let batch = concat_batches(&self.schema(), &self.batches())?;
        let result = TableMaterialization {
            work: TableMaterializationWork {
                rows: batch.num_rows() as u64,
                array_bytes: batch.get_array_memory_size() as u64,
                misses: 1,
            },
            batch: batch.clone(),
        };
        *cache = Some(batch);
        Ok(result)
    }

    /// Pointer equality fast-path; ordered slice equality preserves no-op reuse.
    pub(super) fn same_contents(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        if self.schema() != other.schema() || self.num_rows() != other.num_rows() {
            return false;
        }
        // Splits may leave different leaf boundaries after a canceled transaction.
        // Compare ordered shared slices without a complete concatenation.
        let left = self.batches();
        let right = other.batches();
        let (mut li, mut ri, mut lo, mut ro) = (0, 0, 0, 0);
        while li < left.len() && ri < right.len() {
            let length = (left[li].num_rows() - lo).min(right[ri].num_rows() - ro);
            if !crate::store::batches_equal(
                &left[li].slice(lo, length),
                &right[ri].slice(ro, length),
            ) {
                return false;
            }
            lo += length;
            ro += length;
            if lo == left[li].num_rows() {
                li += 1;
                lo = 0;
            }
            if ro == right[ri].num_rows() {
                ri += 1;
                ro = 0;
            }
        }
        li == left.len() && ri == right.len()
    }

    /// Apply each touched leaf once. All mutation belongs to a private candidate.
    pub(super) fn apply(&self, delta: &Bag) -> Result<TableUpdate> {
        if self.schema() != delta.schema() {
            return Err(invalid("bag schemas differ"));
        }
        let mut blocks_work = TableBlockWork {
            total_blocks_before: self.0.blocks.len(),
            ..Default::default()
        };
        if delta.is_empty() {
            blocks_work.reused_blocks = self.0.blocks.len();
            return Ok(TableUpdate {
                rows: self.clone(),
                work: DmlTargetWork {
                    block_update: Some(blocks_work),
                    ..Default::default()
                },
            });
        }
        // Share the immutable root; only modified directory paths are copied.
        let mut next = TableSnapshot {
            schema: self.schema(),
            blocks: self.0.blocks.clone(),
            row_count: self.num_rows(),
            scalar_bytes: self.0.scalar_bytes,
            order: self.0.order.clone(),
            flat: Mutex::new(None),
        };
        let mut work = DmlTargetWork::default();
        let mut prepared = Vec::new();
        match &self.0.order {
            RowOrder::SingleLeaf => {
                // Do not encode multi-image deltas for an admitted singleton type.
                prepared.push(prepare_block(
                    &mut next,
                    &[],
                    delta,
                    &mut blocks_work,
                    &mut work,
                )?);
            }
            RowOrder::Encoded(converter) => {
                let encoded = delta.keyed_rows(converter)?;
                blocks_work.delta_key_rows = encoded.rows.len();
                blocks_work.delta_key_array_bytes = encoded.array_bytes;
                let mut grouped = BTreeMap::<Vec<u8>, Vec<(&[ScalarValue], i64)>>::new();
                for row in encoded.rows {
                    let block_key = self
                        .0
                        .blocks
                        .predecessor(&row.key)
                        .or_else(|| self.0.blocks.first_key_value())
                        .map(|(k, _)| k.clone())
                        .unwrap_or_default();
                    grouped
                        .entry(block_key)
                        .or_default()
                        .push((row.values, row.weight));
                }
                for (key, entries) in grouped {
                    let changes = Bag::from_unique_entries(self.schema(), entries)?;
                    prepared.push(prepare_block(
                        &mut next,
                        &key,
                        &changes,
                        &mut blocks_work,
                        &mut work,
                    )?);
                }
            }
        }
        bag::check_materialization_volume(next.row_count, next.scalar_bytes)?;
        // Check the final global budget before expanding any replacement leaf.
        // Intermediate cross-leaf key moves may temporarily exceed that budget.
        for after in prepared {
            let leaves = after.split(BLOCK_ENTRIES)?;
            if leaves.len() > 1 {
                blocks_work.split_blocks = blocks_work
                    .split_blocks
                    .checked_add(1)
                    .ok_or(Error::Overflow)?;
                blocks_work.split_row_entries_copied = blocks_work
                    .split_row_entries_copied
                    .checked_add(after.len())
                    .ok_or(Error::Overflow)?;
            }
            for leaf in leaves {
                let (key, block) = make_block(leaf, &next.order)?;
                work.delta_update.record_materialization(&block.batch)?;
                if next.blocks.insert(key, Arc::new(block)).is_some() {
                    return Err(invalid("table leaf boundary collision"));
                }
            }
        }
        blocks_work.reused_blocks = self
            .0
            .blocks
            .len()
            .checked_sub(blocks_work.touched_existing_blocks)
            .ok_or(Error::Overflow)?;
        blocks_work.directory_entries_copied = next.blocks.copied_entries()?;
        work.block_update = Some(blocks_work);
        Ok(TableUpdate {
            rows: Self(Arc::new(next)),
            work,
        })
    }
}

/// Compute one leaf's final Bag and volume; defer Arrow expansion until global checks.
fn prepare_block(
    next: &mut TableSnapshot,
    key: &[u8],
    delta: &Bag,
    counts: &mut TableBlockWork,
    work: &mut DmlTargetWork,
) -> Result<Bag> {
    let old = next.blocks.remove(key);
    counts.touched_blocks = counts
        .touched_blocks
        .checked_add(1)
        .ok_or(Error::Overflow)?;
    let before = old
        .as_ref()
        .map(|b| b.rows.clone())
        .unwrap_or_else(|| Bag::empty(Arc::clone(&next.schema)));
    if let Some(old) = old {
        counts.touched_existing_blocks = counts
            .touched_existing_blocks
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        next.row_count = next
            .row_count
            .checked_sub(old.batch.num_rows())
            .ok_or(Error::Overflow)?;
        next.scalar_bytes = next
            .scalar_bytes
            .checked_sub(old.scalar_bytes)
            .ok_or(Error::Overflow)?;
    }
    let after = before.add(delta)?;
    after.validate_relation()?;
    work.delta_update.record_rebuild(before.len())?;
    let volume = after.materialization_volume()?;
    next.row_count = next
        .row_count
        .checked_add(volume.rows)
        .ok_or(Error::Overflow)?;
    next.scalar_bytes = next
        .scalar_bytes
        .checked_add(volume.scalar_bytes)
        .ok_or(Error::Overflow)?;
    Ok(after)
}

/// Materialize sorted leaf contents; its first encoded row defines its directory key.
fn make_block(rows: Bag, order: &RowOrder) -> Result<(Vec<u8>, TableBlock)> {
    let volume = rows.materialization_volume()?;
    let batch = rows.to_batch()?;
    let key = match order {
        RowOrder::SingleLeaf => Vec::new(),
        RowOrder::Encoded(converter) => {
            let encoded = converter.convert_columns(batch.columns())?;
            if batch.num_rows() == 0 {
                return Err(invalid("empty table leaf"));
            }
            encoded.row(0).as_ref().to_vec()
        }
    };
    Ok((
        key,
        TableBlock {
            rows,
            batch,
            scalar_bytes: volume.scalar_bytes,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringDictionaryBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Int16Type, Schema};

    fn ints(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn retained_and_discarded_snapshots_share_only_untouched_leaves() -> Result<()> {
        let input = ints((0..2048).map(Some).collect());
        let before = TableRows::from_batch(input.clone())?.rows;
        let delta = Bag::from_weighted(
            &ints(vec![
                Some(-1),
                Some(0),
                Some(255),
                Some(256),
                Some(2047),
                Some(3000),
                None,
            ]),
            &[1, -1, -1, -1, -1, 1, 1],
        )?;
        let after = before.apply(&delta)?;
        let expected = Bag::from_batch(&input)?.add(&delta)?.to_batch()?;
        assert!(crate::store::batches_equal(
            &after.rows.materialize()?.batch,
            &expected
        ));
        let unchanged_key = before.0.blocks.keys().nth(4).unwrap();
        assert!(Arc::ptr_eq(
            before.0.blocks.get(unchanged_key).unwrap(),
            after.rows.0.blocks.get(unchanged_key).unwrap()
        ));
        assert!(crate::store::batches_equal(
            &before.materialize()?.batch,
            &input
        ));
        let again = before.apply(&delta)?;
        assert!(again.rows.same_contents(&after.rows));
        assert!(before
            .apply(&Bag::from_weighted(&ints(vec![Some(0)]), &[-2])?)
            .is_err());
        assert!(crate::store::batches_equal(
            &before.materialize()?.batch,
            &input
        ));
        assert!(before
            .apply(&Bag::empty(Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                true
            )]))))
            .is_err());
        Ok(())
    }

    #[test]
    fn canceled_split_reuses_contents_across_different_leaf_boundaries() -> Result<()> {
        let before = TableRows::from_batch(ints((0..256).map(Some).collect()))?.rows;
        let insert = Bag::from_batch(&ints(vec![Some(-1)]))?;
        let split = before.apply(&insert)?.rows;
        assert_eq!(split.0.blocks.len(), 2);
        let canceled = split
            .apply(&Bag::from_weighted(&ints(vec![Some(-1)]), &[-1])?)?
            .rows;
        assert_eq!(canceled.0.blocks.len(), 2);
        assert!(before.same_contents(&canceled));
        assert!(canceled.0.flat.lock().unwrap().is_none());
        assert_eq!(canceled.materialize()?.work.misses, 1);
        assert_eq!(canceled.clone().materialize()?.work.misses, 0);
        Ok(())
    }

    #[test]
    fn float_bits_and_dictionary_values_survive_independent_source_encodings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let make = |v: Vec<Option<f64>>| {
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(v))])
        };
        let input = make(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            None,
        ])?;
        let before = TableRows::from_batch(input.clone())?.rows;
        let delta = Bag::from_weighted(
            &make(vec![
                Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                Some(f64::from_bits(0x7ff8_0000_0000_0003)),
            ])?,
            &[-1, 1],
        )?;
        assert_eq!(
            Bag::from_batch(&before.apply(&delta)?.rows.materialize()?.batch)?,
            Bag::from_batch(&input)?.add(&delta)?
        );

        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        )]));
        let dictionary = |values: Vec<String>| -> Result<RecordBatch> {
            let mut builder = StringDictionaryBuilder::<Int16Type>::new();
            for value in values {
                builder.append(value)?;
            }
            Ok(RecordBatch::try_new(
                Arc::clone(&dict_schema),
                vec![Arc::new(builder.finish())],
            )?)
        };
        let input = dictionary((0..600).map(|i| format!("v{i:04}")).collect())?;
        let before = TableRows::from_batch(input.clone())?.rows;
        // The delta's independent dictionary assigns different numeric keys to values.
        let delta =
            Bag::from_weighted(&dictionary(vec!["v0500".into(), "v0600".into()])?, &[-1, 1])?;
        assert_eq!(
            Bag::from_batch(&before.apply(&delta)?.rows.materialize()?.batch)?,
            Bag::from_batch(&input)?.add(&delta)?
        );
        Ok(())
    }

    #[test]
    fn singleton_branch_checks_final_state_and_global_budgets_before_expansion() -> Result<()> {
        let mut before = TableRows::from_batch(ints(vec![Some(1)]))?.rows;
        // Exercise the fallback independently of Arrow's current supported-type list.
        let snapshot = Arc::get_mut(&mut before.0).unwrap();
        snapshot.order = RowOrder::SingleLeaf;
        let key = snapshot.blocks.first_key_value().unwrap().0.clone();
        let block = snapshot.blocks.remove(&key).unwrap();
        snapshot.blocks.insert(Vec::new(), block);
        let delta = Bag::from_weighted(&ints(vec![Some(1), Some(2)]), &[-1, 1])?;
        let after = before.apply(&delta)?.rows;
        assert_eq!(
            Bag::from_batch(&after.materialize()?.batch)?,
            Bag::from_batch(&ints(vec![Some(2)]))?
        );
        let empty = after
            .apply(&Bag::from_weighted(&ints(vec![Some(2)]), &[-1])?)?
            .rows;
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.materialize()?.batch.schema(), before.schema());

        let before = TableRows::from_batch(ints((0..512).map(Some).collect()))?.rows;
        let too_many = Bag::from_weighted(
            &ints(vec![Some(0)]),
            &[(crate::MAX_MATERIALIZED_ROWS - 256) as i64],
        )?;
        assert!(before.apply(&too_many).is_err());
        assert_eq!(before.num_rows(), 512);
        let wide_schema = Arc::new(Schema::new(
            (0..32)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let wide = RecordBatch::try_new(
            wide_schema,
            (0..32)
                .map(|_| {
                    Arc::new(Int64Array::from_iter_values(0..512))
                        as datafusion::arrow::array::ArrayRef
                })
                .collect(),
        )?;
        let wide_table = TableRows::from_batch(wide.clone())?.rows;
        let too_wide = Bag::from_weighted(
            &wide.slice(0, 1),
            &[(crate::MAX_MATERIALIZED_ROWS / 2) as i64],
        )?;
        assert!(wide_table.apply(&too_wide).is_err());
        assert_eq!(wide_table.num_rows(), 512);
        let input = ints(vec![Some(0)]);
        assert!(before
            .apply(&Bag::from_weighted(&input, &[i64::MAX])?)
            .is_err());
        Ok(())
    }
}

#[cfg(test)]
mod randomized_tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn deterministic_mixed_updates_and_discarded_candidates_match_an_independent_multiset(
    ) -> Result<()> {
        let mut model = (0..1024)
            .map(|k| (Some(k), 1usize))
            .collect::<BTreeMap<_, _>>();
        let mut current = TableRows::from_batch(batch((0..1024).map(Some).collect()))?.rows;
        let old = current.clone();
        let mut seed = 0x7a92c66du64;
        for round in 0usize..300 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let old_key = *model.keys().nth((seed as usize) % model.len()).unwrap();
            let new_key = if seed.is_multiple_of(11) {
                None
            } else {
                Some(((seed >> 24) % 1600) as i64 - 100)
            };
            let delta = Bag::from_weighted(&batch(vec![old_key, new_key]), &[-1, 1])?;
            let candidate = current.apply(&delta)?;
            if !round.is_multiple_of(5) {
                let remaining = model.get_mut(&old_key).unwrap();
                *remaining -= 1;
                if *remaining == 0 {
                    model.remove(&old_key);
                }
                *model.entry(new_key).or_default() += 1;
                current = candidate.rows;
            }
            if round.is_multiple_of(17) {
                let expected = batch(
                    model
                        .iter()
                        .flat_map(|(key, count)| std::iter::repeat_n(*key, *count))
                        .collect(),
                );
                assert!(
                    crate::store::batches_equal(&current.materialize()?.batch, &expected),
                    "round {round}"
                );
                assert!(current
                    .0
                    .blocks
                    .values()
                    .all(|leaf| leaf.rows.len() <= BLOCK_ENTRIES));
            }
        }
        let expected = batch(
            model
                .iter()
                .flat_map(|(key, count)| std::iter::repeat_n(*key, *count))
                .collect(),
        );
        assert!(crate::store::batches_equal(
            &current.materialize()?.batch,
            &expected
        ));
        assert!(crate::store::batches_equal(
            &old.materialize()?.batch,
            &batch((0..1024).map(Some).collect())
        ));
        Ok(())
    }

    #[test]
    fn bulk_key_moves_encode_multiple_batches_and_check_only_the_final_endpoint() -> Result<()> {
        let before = TableRows::from_batch(batch((0..2048).map(Some).collect()))?.rows;
        let changes = batch(
            (0..2048)
                .map(Some)
                .chain((10000..12048).map(Some))
                .collect(),
        );
        let weights = std::iter::repeat_n(-1, 2048)
            .chain(std::iter::repeat_n(1, 2048))
            .collect::<Vec<_>>();
        let delta = Bag::from_weighted(&changes, &weights)?;
        let after = before.apply(&delta)?;
        assert_eq!(after.work.block_update.unwrap().delta_key_rows, 4096);
        assert!(crate::store::batches_equal(
            &after.rows.materialize()?.batch,
            &batch((10000..12048).map(Some).collect())
        ));
        assert_eq!(before.num_rows(), 2048);
        Ok(())
    }
}

#[cfg(test)]
mod zero_column_tests {
    use super::*;
    use datafusion::arrow::array::RecordBatchOptions;
    use datafusion::arrow::datatypes::Schema;

    #[test]
    fn zero_columns_preserve_multiplicity_through_replacement_and_flattening() -> Result<()> {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )?;
        let before = TableRows::from_batch(batch.clone())?.rows;
        let delta = Bag::from_weighted(&batch.slice(0, 1), &[-1])?;
        let after = before.apply(&delta)?.rows;
        assert_eq!(after.num_rows(), 2);
        assert_eq!(after.materialize()?.batch.num_rows(), 2);
        assert_eq!(before.materialize()?.batch.num_rows(), 3);
        Ok(())
    }
}
