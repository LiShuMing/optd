//! Shared positive row blocks; only touched leaves are rebuilt by signed changes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;

use crate::bag::{self, Bag};
use crate::error::invalid;
use crate::state_directory::StateDirectory;
use crate::work::{
    RelationWork, RowBlockWork, RowBootstrapWork, RowEncodingWork, RowMaterializationWork,
    RowUpdateWork,
};
use crate::{Error, Result};

/// Bound historical distinct-entry copying; expanded duplicate rows may be larger.
const BLOCK_ENTRIES: usize = 256;

/// Unsupported row encoders retain Bag's existing singleton admission behavior.
#[derive(Clone)]
enum RowOrder {
    Encoded(Arc<RowConverter>),
    SingleLeaf,
}

/// SQL targets retain their eager contract until measured lazy providers are ready.
#[derive(Clone, Copy)]
enum LeafCachePolicy {
    OnUpdate,
    OnDemand,
}

/// Shared weighted row state; a graph consumer alone demands its expanded cache.
struct RowBlock {
    rows: Bag,
    batch: Mutex<Option<RecordBatch>>,
    row_count: usize,
    scalar_bytes: usize,
}

/// Leaf construction separates identity encoding from actual Arrow expansion.
struct BlockBuild {
    key: Vec<u8>,
    block: RowBlock,
    materialization: RelationWork,
    validation: RowEncodingWork,
}

/// Ordered shared scan batches and work actually performed to obtain them.
struct LeafBatches {
    batches: Vec<RecordBatch>,
    work: RelationWork,
}

/// Contents of one immutable positive revision, shared across candidate clones.
struct RowSnapshot {
    schema: SchemaRef,
    blocks: StateDirectory<Arc<RowBlock>>,
    row_count: usize,
    scalar_bytes: usize,
    order: RowOrder,
    leaf_policy: LeafCachePolicy,
    // Flatten only at consumers that require one batch. No lock crosses await.
    flat: Mutex<Option<RecordBatch>>,
}

/// Share an immutable positive relation and its optional complete Arrow cache.
#[derive(Clone)]
pub(crate) struct RowBlocks(Arc<RowSnapshot>);

/// Actual single-batch cache result, with a measured miss rather than inferred work.
pub(crate) struct RowMaterialization {
    pub(crate) batch: RecordBatch,
    pub(crate) work: RowMaterializationWork,
    pub(crate) leaf_work: RelationWork,
}

/// Imported leaves and the actual conversion work used to build them.
pub(crate) struct RowBootstrap {
    pub(crate) rows: RowBlocks,
    pub(crate) work: RowBootstrapWork,
    pub(crate) leaf_validation: RowEncodingWork,
}

/// A candidate positive revision and its actual leaf-update work.
pub(crate) struct RowUpdate {
    pub(crate) rows: RowBlocks,
    pub(crate) work: RowUpdateWork,
}

impl RowBlocks {
    /// Bootstrap once from an imported/created batch; later statements use leaves.
    pub(crate) fn from_batch(batch: RecordBatch) -> Result<RowBootstrap> {
        Self::bootstrap(batch, LeafCachePolicy::OnUpdate)
    }

    /// Graph consumers propagate weighted deltas without expanding updated leaves.
    pub(crate) fn from_relation_batch(batch: RecordBatch) -> Result<RowBootstrap> {
        Self::bootstrap(batch, LeafCachePolicy::OnDemand)
    }

    fn bootstrap(batch: RecordBatch, leaf_policy: LeafCachePolicy) -> Result<RowBootstrap> {
        let rows = Bag::from_batch(&batch)?;
        let volume = rows.materialization_volume()?;
        let mut work = RowBootstrapWork {
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
        let mut snapshot = RowSnapshot {
            schema: batch.schema(),
            blocks: StateDirectory::new(),
            row_count: volume.rows,
            scalar_bytes: volume.scalar_bytes,
            order,
            leaf_policy,
            flat: Mutex::new(Some(batch)),
        };
        let mut leaf_validation = RowEncodingWork::default();
        for leaf in rows.split(BLOCK_ENTRIES)? {
            let built = make_block(leaf, &snapshot.order, snapshot.leaf_policy)?;
            work.leaf_materializations = work
                .leaf_materializations
                .checked_add(built.materialization.materializations as u64)
                .ok_or(Error::Overflow)?;
            work.materialized_rows = work
                .materialized_rows
                .checked_add(built.materialization.materialized_rows as u64)
                .ok_or(Error::Overflow)?;
            work.materialized_array_bytes = work
                .materialized_array_bytes
                .checked_add(built.materialization.materialized_array_bytes as u64)
                .ok_or(Error::Overflow)?;
            leaf_validation.absorb(&built.validation)?;
            snapshot.blocks.insert(built.key, Arc::new(built.block));
        }
        Ok(RowBootstrap {
            rows: Self(Arc::new(snapshot)),
            work,
            leaf_validation,
        })
    }

    /// Record a demanded complete concatenation; repeated reads share its cache.
    pub(crate) fn materialize_recorded(
        &self,
        work: &mut RowMaterializationWork,
        leaf_work: &mut RelationWork,
    ) -> Result<RecordBatch> {
        let result = self.materialize()?;
        work.absorb(&result.work)?;
        leaf_work.absorb(&result.leaf_work)?;
        Ok(result.batch)
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0.schema)
    }
    pub(crate) fn num_rows(&self) -> usize {
        self.0.row_count
    }
    pub(crate) fn num_columns(&self) -> usize {
        self.0.schema.fields().len()
    }

    /// SQL MemTables consume their eager leaves; propagate any cache/conversion error.
    /// Graph callers use recorded complete-demand boundaries instead.
    pub(crate) fn batches(&self) -> Result<Vec<RecordBatch>> {
        Ok(self.leaf_batches()?.batches)
    }

    fn leaf_batches(&self) -> Result<LeafBatches> {
        let mut result = LeafBatches {
            batches: Vec::new(),
            work: RelationWork::default(),
        };
        if self.0.blocks.is_empty() {
            result.batches.push(RecordBatch::new_empty(self.schema()));
        }
        for block in self.0.blocks.values() {
            let mut cache = block
                .batch
                .lock()
                .map_err(|_| invalid("row leaf cache poisoned"))?;
            let batch = match cache.as_ref() {
                Some(batch) => batch.clone(),
                None => {
                    let batch = block.rows.to_batch()?;
                    result.work.record_materialization(&batch)?;
                    *cache = Some(batch.clone());
                    batch
                }
            };
            result.batches.push(batch);
        }
        Ok(result)
    }

    /// Cache one concatenation per immutable revision, including empty revisions.
    pub(crate) fn materialize(&self) -> Result<RowMaterialization> {
        let mut cache = self
            .0
            .flat
            .lock()
            .map_err(|_| invalid("table flat cache poisoned"))?;
        if let Some(batch) = cache.as_ref() {
            return Ok(RowMaterialization {
                batch: batch.clone(),
                work: RowMaterializationWork::default(),
                leaf_work: RelationWork::default(),
            });
        }
        let leaves = self.leaf_batches()?;
        let batch = concat_batches(&self.schema(), &leaves.batches)?;
        let result = RowMaterialization {
            work: RowMaterializationWork {
                rows: batch.num_rows() as u64,
                array_bytes: batch.get_array_memory_size() as u64,
                misses: 1,
            },
            batch: batch.clone(),
            leaf_work: leaves.work,
        };
        *cache = Some(batch);
        Ok(result)
    }

    /// Pointer equality fast-path; ordered slice equality preserves no-op reuse.
    pub(crate) fn same_contents(&self, other: &Self) -> Result<bool> {
        if Arc::ptr_eq(&self.0, &other.0) {
            return Ok(true);
        }
        if self.schema() != other.schema() || self.num_rows() != other.num_rows() {
            return Ok(false);
        }
        // Splits may leave different leaf boundaries after a canceled transaction.
        // Compare ordered shared slices without a complete concatenation.
        let left = self.batches()?;
        let right = other.batches()?;
        let (mut li, mut ri, mut lo, mut ro) = (0, 0, 0, 0);
        while li < left.len() && ri < right.len() {
            let length = (left[li].num_rows() - lo).min(right[ri].num_rows() - ro);
            if !crate::store::batches_equal(
                &left[li].slice(lo, length),
                &right[ri].slice(ro, length),
            ) {
                return Ok(false);
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
        Ok(li == left.len() && ri == right.len())
    }

    /// Apply each touched leaf once. All mutation belongs to a private candidate.
    pub(crate) fn apply(&self, delta: &Bag) -> Result<RowUpdate> {
        if self.schema() != delta.schema() {
            return Err(invalid("bag schemas differ"));
        }
        let mut blocks_work = RowBlockWork {
            total_blocks_before: self.0.blocks.len(),
            ..Default::default()
        };
        if delta.is_empty() {
            blocks_work.reused_blocks = self.0.blocks.len();
            return Ok(RowUpdate {
                rows: self.clone(),
                work: RowUpdateWork {
                    block_update: blocks_work,
                    ..Default::default()
                },
            });
        }
        // Share the immutable root; only modified directory paths are copied.
        let mut next = RowSnapshot {
            schema: self.schema(),
            blocks: self.0.blocks.clone(),
            row_count: self.num_rows(),
            scalar_bytes: self.0.scalar_bytes,
            order: self.0.order.clone(),
            leaf_policy: self.0.leaf_policy,
            flat: Mutex::new(None),
        };
        let mut work = RowUpdateWork::default();
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
                let built = make_block(leaf, &next.order, next.leaf_policy)?;
                work.delta_update.absorb(&built.materialization)?;
                work.leaf_validation.absorb(&built.validation)?;
                if next
                    .blocks
                    .insert(built.key, Arc::new(built.block))
                    .is_some()
                {
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
        work.block_update = blocks_work;
        Ok(RowUpdate {
            rows: Self(Arc::new(next)),
            work,
        })
    }
}

/// Compute one leaf's final Bag and volume; defer Arrow expansion until global checks.
fn prepare_block(
    next: &mut RowSnapshot,
    key: &[u8],
    delta: &Bag,
    counts: &mut RowBlockWork,
    work: &mut RowUpdateWork,
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
            .checked_sub(old.row_count)
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

/// Preflight exact identity before publication, expanding only eager SQL leaves.
fn make_block(rows: Bag, order: &RowOrder, policy: LeafCachePolicy) -> Result<BlockBuild> {
    let volume = rows.materialization_volume()?;
    let mut materialization = RelationWork::default();
    let mut validation = RowEncodingWork::default();
    let (boundary_batch, cached) = match policy {
        LeafCachePolicy::OnUpdate => {
            let batch = rows.to_batch()?;
            materialization.record_materialization(&batch)?;
            (batch.clone(), Some(batch))
        }
        LeafCachePolicy::OnDemand => {
            // Bag retains its exact sort-encoding tie checks without expanding weights.
            let batch = rows.distinct_batch()?;
            validation.record(&batch)?;
            (batch, None)
        }
    };
    let key = match order {
        RowOrder::SingleLeaf => Vec::new(),
        RowOrder::Encoded(converter) => {
            let encoded = converter.convert_columns(boundary_batch.columns())?;
            if boundary_batch.num_rows() == 0 {
                return Err(invalid("empty table leaf"));
            }
            encoded.row(0).as_ref().to_vec()
        }
    };
    Ok(BlockBuild {
        key,
        block: RowBlock {
            rows,
            batch: Mutex::new(cached),
            row_count: volume.rows,
            scalar_bytes: volume.scalar_bytes,
        },
        materialization,
        validation,
    })
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
        let before = RowBlocks::from_batch(input.clone())?.rows;
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
        assert!(again.rows.same_contents(&after.rows)?);
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
        let before = RowBlocks::from_batch(ints((0..256).map(Some).collect()))?.rows;
        let insert = Bag::from_batch(&ints(vec![Some(-1)]))?;
        let split = before.apply(&insert)?.rows;
        assert_eq!(split.0.blocks.len(), 2);
        let canceled = split
            .apply(&Bag::from_weighted(&ints(vec![Some(-1)]), &[-1])?)?
            .rows;
        assert_eq!(canceled.0.blocks.len(), 2);
        assert!(before.same_contents(&canceled)?);
        assert!(canceled.0.flat.lock().unwrap().is_none());
        assert_eq!(canceled.materialize()?.work.misses, 1);
        assert_eq!(canceled.clone().materialize()?.work.misses, 0);
        Ok(())
    }

    #[test]
    fn float_bits_and_dictionary_values_survive_independent_source_encodings() -> Result<()> {
        for policy in [LeafCachePolicy::OnUpdate, LeafCachePolicy::OnDemand] {
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
            let before = RowBlocks::bootstrap(input.clone(), policy)?.rows;
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
            let before = RowBlocks::bootstrap(input.clone(), policy)?.rows;
            // The delta's independent dictionary assigns different numeric keys to values.
            let delta =
                Bag::from_weighted(&dictionary(vec!["v0500".into(), "v0600".into()])?, &[-1, 1])?;
            assert_eq!(
                Bag::from_batch(&before.apply(&delta)?.rows.materialize()?.batch)?,
                Bag::from_batch(&input)?.add(&delta)?
            );
        }
        Ok(())
    }

    #[test]
    fn singleton_branch_checks_final_state_and_global_budgets_before_expansion() -> Result<()> {
        for policy in [LeafCachePolicy::OnUpdate, LeafCachePolicy::OnDemand] {
            let mut before = RowBlocks::bootstrap(ints(vec![Some(1)]), policy)?.rows;
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

            let before = RowBlocks::bootstrap(ints((0..512).map(Some).collect()), policy)?.rows;
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
            let wide_table = RowBlocks::bootstrap(wide.clone(), policy)?.rows;
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
        }
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
        for policy in [LeafCachePolicy::OnUpdate, LeafCachePolicy::OnDemand] {
            let mut model = (0..1024)
                .map(|k| (Some(k), 1usize))
                .collect::<BTreeMap<_, _>>();
            let mut current =
                RowBlocks::bootstrap(batch((0..1024).map(Some).collect()), policy)?.rows;
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
        }
        Ok(())
    }

    #[test]
    fn bulk_key_moves_encode_multiple_batches_and_check_only_the_final_endpoint() -> Result<()> {
        for policy in [LeafCachePolicy::OnUpdate, LeafCachePolicy::OnDemand] {
            let before = RowBlocks::bootstrap(batch((0..2048).map(Some).collect()), policy)?.rows;
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
            assert_eq!(after.work.block_update.delta_key_rows, 4096);
            assert!(crate::store::batches_equal(
                &after.rows.materialize()?.batch,
                &batch((10000..12048).map(Some).collect())
            ));
            assert_eq!(before.num_rows(), 2048);
        }
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
        for policy in [LeafCachePolicy::OnUpdate, LeafCachePolicy::OnDemand] {
            let batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(3)),
            )?;
            let before = RowBlocks::bootstrap(batch.clone(), policy)?.rows;
            let delta = Bag::from_weighted(&batch.slice(0, 1), &[-1])?;
            let after = before.apply(&delta)?.rows;
            assert_eq!(after.num_rows(), 2);
            assert_eq!(after.materialize()?.batch.num_rows(), 2);
            assert_eq!(before.materialize()?.batch.num_rows(), 3);
        }
        Ok(())
    }
}

#[cfg(test)]
mod lazy_leaf_tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn duplicate_weights_expand_only_on_demand_and_repeated_reads_share_the_cache() -> Result<()> {
        for n in [1024, 8192] {
            let imported = RowBlocks::from_relation_batch(batch(vec![7; n]))?;
            assert_eq!(imported.work.leaf_materializations, 0);
            assert_eq!(imported.leaf_validation.rows, 1);
            let before = imported.rows;
            let delta = Bag::from_batch(&batch(vec![7]))?;
            let candidate = before.apply(&delta)?;
            assert_eq!(candidate.work.delta_update.materialized_rows, 0);
            assert_eq!(candidate.work.leaf_validation.rows, 1);
            assert!(candidate
                .rows
                .0
                .blocks
                .values()
                .all(|b| b.batch.lock().unwrap().is_none()));
            let demanded = candidate.rows.materialize()?;
            assert_eq!(demanded.batch.num_rows(), n + 1);
            assert_eq!(demanded.leaf_work.materializations, 1);
            assert_eq!(demanded.leaf_work.materialized_rows, n + 1);
            assert_eq!(demanded.work.misses, 1);
            let again = candidate.rows.clone().materialize()?;
            assert_eq!(again.leaf_work, RelationWork::default());
            assert_eq!(again.work.misses, 0);
            assert_eq!(before.materialize()?.batch.num_rows(), n);
            assert!(before
                .0
                .blocks
                .values()
                .all(|b| b.batch.lock().unwrap().is_none()));
        }
        Ok(())
    }

    #[test]
    fn complete_demand_reuses_unchanged_leaf_caches_and_counts_only_missing_leaves() -> Result<()> {
        let before = RowBlocks::from_relation_batch(batch((0..1024).collect()))?.rows;
        let initial = before.leaf_batches()?;
        assert_eq!(initial.work.materialized_rows, 1024);
        assert_eq!(before.leaf_batches()?.work.materializations, 0);
        let delta = Bag::from_batch(&batch(vec![1]))?;
        let candidate = before.apply(&delta)?;
        assert_eq!(candidate.work.delta_update.materialized_rows, 0);
        let demand = candidate.rows.materialize()?;
        assert_eq!(demand.leaf_work.materialized_rows, 257);
        assert_eq!(demand.leaf_work.materializations, 1);
        assert_eq!(demand.work.rows, 1025);
        assert_eq!(
            Bag::from_batch(&demand.batch)?,
            Bag::from_batch(&before.materialize()?.batch)?.add(&delta)?
        );
        assert_eq!(before.num_rows(), 1024);
        let key = before.0.blocks.keys().last().unwrap();
        assert!(Arc::ptr_eq(
            before.0.blocks.get(key).unwrap(),
            candidate.rows.0.blocks.get(key).unwrap()
        ));
        Ok(())
    }
}
