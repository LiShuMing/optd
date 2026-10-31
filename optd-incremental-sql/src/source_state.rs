//! Shared weighted raw sources; hashes select buckets, exact rows decide identity.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;

use crate::bag::Bag;
use crate::error::invalid;
use crate::state_directory::StateDirectory;
use crate::work::{RelationWork, SourceStateWork};
use crate::{Error, Result};

type RowHash = fn(&[ScalarValue]) -> u64;

/// One immutable source revision, including a demand-only complete Bag cache.
struct SourceSnapshot {
    schema: SchemaRef,
    distinct_rows: usize,
    buckets: StateDirectory<Arc<Bag>>,
    hash: RowHash,
    // Only full snapshot/export consumers acquire this lock; never across await.
    flat: Mutex<Option<Bag>>,
}

/// Cloning shares a revision; updates copy only touched collision buckets and paths.
#[derive(Clone)]
pub(crate) struct SourceState(Arc<SourceSnapshot>);

/// A collected snapshot and actual work; cache hits report no collection.
pub(crate) struct SourceCollection {
    pub(crate) bag: Bag,
    pub(crate) collections: usize,
    pub(crate) copied_entries: usize,
}

impl SourceState {
    /// Build once from an existing positive Bag, retaining its initial scan cache.
    pub(crate) fn new(bag: Bag) -> Result<Self> {
        Self::with_hash(bag, row_hash)
    }

    fn with_hash(bag: Bag, hash: RowHash) -> Result<Self> {
        bag.validate_relation()?;
        let buckets = partition(&bag, hash)?
            .into_iter()
            .map(|(key, rows)| (key, Arc::new(rows)))
            .collect();
        Ok(Self(Arc::new(SourceSnapshot {
            schema: bag.schema(),
            distinct_rows: bag.len(),
            buckets,
            hash,
            flat: Mutex::new(Some(bag)),
        })))
    }

    /// Validate the ordered prefix by probing incoming rows, without expansion.
    pub(crate) fn check_staged_addition(
        &self,
        staged: Option<&Bag>,
        incoming: &Bag,
        work: &mut SourceStateWork,
    ) -> Result<()> {
        if self.0.schema != incoming.schema() || staged.is_some_and(|s| self.0.schema != s.schema())
        {
            return Err(invalid("bag schemas differ"));
        }
        for (row, weight) in incoming.entries() {
            let key = (self.0.hash)(row).to_be_bytes();
            let committed = self.0.buckets.get(&key).map_or(0, |b| b.weight(row));
            let staged_weight = staged.map_or(0, |b| b.weight(row));
            committed
                .checked_add(staged_weight)
                .and_then(|current| current.checked_add(weight))
                .ok_or(Error::Overflow)?;
            work.record_validation_probe()?;
        }
        Ok(())
    }

    /// Apply the consolidated delta once per touched bucket; candidates stay private.
    /// No Arrow encoding or materialization budget applies to stored raw weights.
    pub(crate) fn apply(
        &self,
        delta: &Bag,
        maps: &mut RelationWork,
        work: &mut SourceStateWork,
    ) -> Result<Self> {
        if self.0.schema != delta.schema() {
            return Err(invalid("bag schemas differ"));
        }
        if delta.is_empty() {
            return Ok(self.clone());
        }
        let mut buckets = self.0.buckets.clone();
        let mut distinct_rows = self.0.distinct_rows;
        for (key, change) in partition(delta, self.0.hash)? {
            let old = self
                .0
                .buckets
                .get(&key)
                .map(|b| b.as_ref().clone())
                .unwrap_or_else(|| Bag::empty(self.0.schema.clone()));
            let next = old.add(&change)?;
            next.validate_relation()?;
            distinct_rows = distinct_rows
                .checked_sub(old.len())
                .and_then(|n| n.checked_add(next.len()))
                .ok_or(Error::Overflow)?;
            maps.record_rebuild(old.len())?;
            work.record_touched_bucket()?;
            if next.is_empty() {
                buckets.remove(&key);
            } else {
                buckets.insert(key, Arc::new(next));
            }
        }
        work.record_update(buckets.copied_entries()?)?;
        Ok(Self(Arc::new(SourceSnapshot {
            schema: self.0.schema.clone(),
            distinct_rows,
            buckets,
            hash: self.0.hash,
            flat: Mutex::new(None),
        })))
    }

    /// Collect exact rows on explicit demand; hash traversal never defines output order.
    /// Arrow expansion and canonical ordering remain Bag's responsibility.
    pub(crate) fn snapshot(&self) -> Result<SourceCollection> {
        let mut cache = self
            .0
            .flat
            .lock()
            .map_err(|_| invalid("source snapshot cache poisoned"))?;
        if let Some(bag) = cache.as_ref() {
            return Ok(SourceCollection {
                bag: bag.clone(),
                collections: 0,
                copied_entries: 0,
            });
        }
        let bag = Bag::from_unique_entries(
            self.0.schema.clone(),
            self.0.buckets.values().flat_map(|bucket| bucket.entries()),
        )?;
        let result = SourceCollection {
            bag: bag.clone(),
            collections: 1,
            copied_entries: bag.len(),
        };
        *cache = Some(bag);
        Ok(result)
    }
}

/// Equal ScalarValue rows always select the same bucket; collisions stay exact Bags.
fn row_hash(row: &[ScalarValue]) -> u64 {
    let mut hash = DefaultHasher::new();
    row.hash(&mut hash);
    hash.finish()
}

/// Group borrowed entries before constructing each touched map once.
fn partition(bag: &Bag, hash: RowHash) -> Result<BTreeMap<Vec<u8>, Bag>> {
    let mut groups = BTreeMap::<Vec<u8>, Vec<(&[ScalarValue], i64)>>::new();
    for (row, weight) in bag.entries() {
        groups
            .entry(hash(row).to_be_bytes().to_vec())
            .or_default()
            .push((row, weight));
    }
    groups
        .into_iter()
        .map(|(key, entries)| Ok((key, Bag::from_unique_entries(bag.schema(), entries)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }
    fn row(v: Option<i64>) -> Vec<ScalarValue> {
        vec![ScalarValue::Int64(v)]
    }
    fn bag(entries: &[(Option<i64>, i64)]) -> Bag {
        let rows = entries
            .iter()
            .map(|(v, w)| (row(*v), *w))
            .collect::<Vec<_>>();
        Bag::from_unique_entries(schema(), rows.iter().map(|(r, w)| (r.as_slice(), *w))).unwrap()
    }
    fn update(source: &SourceState, delta: &Bag) -> (SourceState, RelationWork, SourceStateWork) {
        let mut maps = RelationWork::default();
        let mut work = SourceStateWork::default();
        source
            .check_staged_addition(None, delta, &mut work)
            .unwrap();
        let next = source.apply(delta, &mut maps, &mut work).unwrap();
        (next, maps, work)
    }

    #[test]
    fn forced_collisions_keep_exact_null_duplicate_move_and_delete_identity() {
        let initial = bag(&[(None, 2), (Some(1), 3), (Some(2), 1)]);
        let source = SourceState::with_hash(initial.clone(), |_| 0).unwrap();
        let delta = bag(&[(None, -1), (Some(1), -3), (Some(3), 4)]);
        let (next, maps, work) = update(&source, &delta);
        assert_eq!(maps.map_rebuilds, 1);
        assert_eq!(maps.old_row_entries_copied, 3);
        assert_eq!(work.touched_buckets, 1);
        assert_eq!(work.validation_probes, 3);
        assert_eq!(
            next.snapshot().unwrap().bag,
            bag(&[(None, 1), (Some(2), 1), (Some(3), 4)])
        );
        assert_eq!(source.snapshot().unwrap().bag, initial);
        assert!(source
            .apply(
                &bag(&[(Some(99), -1)]),
                &mut Default::default(),
                &mut Default::default()
            )
            .is_err());
        let (empty, _, _) = update(&next, &bag(&[(None, -1), (Some(2), -1), (Some(3), -4)]));
        assert!(empty.0.buckets.is_empty());
        assert!(empty.snapshot().unwrap().bag.is_empty());
    }

    #[test]
    fn scalar_float_identity_survives_hash_collisions_and_deletions() {
        use datafusion::arrow::array::{Float64Array, RecordBatch};
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let values = vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            None,
        ];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap();
        let initial = Bag::from_batch(&batch).unwrap();
        for hash in [row_hash as RowHash, (|_: &[ScalarValue]| 0) as RowHash] {
            let source = SourceState::with_hash(initial.clone(), hash).unwrap();
            let change = Bag::from_weighted(&batch.slice(0, 1), &[-1]).unwrap();
            let (next, _, _) = update(&source, &change);
            assert_eq!(next.snapshot().unwrap().bag, initial.add(&change).unwrap());
            assert_eq!(next.snapshot().unwrap().bag.len(), 4);
            assert_eq!(source.snapshot().unwrap().bag, initial);
        }
    }

    #[test]
    fn fixed_bucket_work_and_demand_only_collection_with_growing_untouched_rows() {
        for n in [1024, 8192] {
            let entries = (0..n).map(|i| (Some(i), 1)).collect::<Vec<_>>();
            let source = SourceState::new(bag(&entries)).unwrap();
            let (next, maps, work) = update(&source, &bag(&[(Some(0), 1)]));
            assert_eq!(work.updates, 1);
            assert_eq!(work.snapshot_collections, 0);
            assert_eq!(maps.old_row_entries_copied, 1);
            assert_eq!(maps.materializations, 0);
            assert!(work.directory_entries_copied <= 3 * (n.ilog2() as usize + 1));
            let collected = next.snapshot().unwrap();
            assert_eq!(collected.collections, 1);
            assert_eq!(collected.copied_entries, n as usize);
            assert_eq!(next.snapshot().unwrap().collections, 0);
            assert_eq!(next.snapshot().unwrap().copied_entries, 0);
            assert_eq!(source.snapshot().unwrap().bag.weight(&row(Some(0))), 1);
        }
    }

    #[test]
    fn extreme_weights_and_prefix_overflow_preserve_lazy_storage_contract() {
        let source = SourceState::new(bag(&[(Some(0), 1)])).unwrap();
        let delta = bag(&[(Some(0), i64::MAX - 1)]);
        let (huge, _, _) = update(&source, &delta);
        assert!(huge.snapshot().unwrap().bag.to_batch().is_err());
        let (small, _, _) = update(&huge, &bag(&[(Some(0), -(i64::MAX - 1))]));
        assert_eq!(small.snapshot().unwrap().bag, bag(&[(Some(0), 1)]));
        assert!(matches!(
            source.check_staged_addition(
                Some(&delta),
                &bag(&[(Some(0), 1)]),
                &mut Default::default()
            ),
            Err(Error::Overflow)
        ));
        assert!(matches!(
            source.check_staged_addition(
                None,
                &bag(&[(Some(0), i64::MAX)]),
                &mut Default::default()
            ),
            Err(Error::Overflow)
        ));
        // Negative intermediate prefixes are admitted; final deletion is rejected.
        let prefix = bag(&[(Some(99), i64::MIN)]);
        source
            .check_staged_addition(None, &prefix, &mut Default::default())
            .unwrap();
        source
            .check_staged_addition(
                Some(&prefix),
                &bag(&[(Some(99), i64::MAX)]),
                &mut Default::default(),
            )
            .unwrap();
        assert!(source
            .apply(&prefix, &mut Default::default(), &mut Default::default())
            .is_err());
    }

    #[test]
    fn deterministic_mixed_revisions_match_independent_map_with_collisions_and_discard() {
        for hash in [row_hash as RowHash, (|_: &[ScalarValue]| 0) as RowHash] {
            let mut source = SourceState::with_hash(Bag::empty(schema()), hash).unwrap();
            let mut expected = HashMap::<Vec<ScalarValue>, i64>::new();
            let mut seed = 7919u64;
            let mut retained = Vec::new();
            for step in 0..1500 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = if seed.is_multiple_of(17) {
                    None
                } else {
                    Some(((seed >> 8) % 97) as i64)
                };
                let row = row(key);
                let weight = if seed & 7 == 0 && expected.contains_key(&row) {
                    -1
                } else {
                    1
                };
                let change = bag(&[(key, weight)]);
                let (next, _, _) = update(&source, &change);
                let mut oracle = expected.clone();
                let value = oracle.get(&row).copied().unwrap_or_default() + weight;
                if value == 0 {
                    oracle.remove(&row);
                } else {
                    oracle.insert(row, value);
                }
                let actual = next.snapshot().unwrap().bag;
                assert_eq!(
                    actual
                        .entries()
                        .map(|(r, w)| (r.to_vec(), w))
                        .collect::<HashMap<_, _>>(),
                    oracle
                );
                if step % 29 == 0 {
                    retained.push((source.clone(), expected.clone()));
                }
                if step % 7 != 0 {
                    source = next;
                    expected = oracle;
                }
            }
            for (old, oracle) in retained {
                assert_eq!(
                    old.snapshot()
                        .unwrap()
                        .bag
                        .entries()
                        .map(|(r, w)| (r.to_vec(), w))
                        .collect::<HashMap<_, _>>(),
                    oracle
                );
            }
        }
    }
}
