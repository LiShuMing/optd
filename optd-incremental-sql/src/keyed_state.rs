//! Derived typed indexes for complete affected-key snapshots, never full filters.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::arrow::datatypes::{DataType, SchemaRef};

use crate::bag::{Bag, MAX_MATERIALIZED_BYTES, MAX_MATERIALIZED_ROWS};
use crate::group_state::{encode_keys, partition};
use crate::planner::SnapshotVersion;
use crate::{Error, Result};

type Key = Vec<u8>;

#[derive(Clone, Debug)]
struct Bucket {
    bag: Bag,
    // Before must retain the actual bootstrap input sequence, including peers.
    batch: RecordBatch,
}

#[derive(Clone, Debug)]
pub(crate) struct KeyedState {
    schemas: Vec<SchemaRef>,
    key_types: Vec<DataType>,
    inputs: Vec<BTreeMap<Key, Arc<Bucket>>>,
    // Window Before is committed output, not a potentially tie-reassigning run.
    outputs: Option<(SchemaRef, BTreeMap<Key, Arc<Bag>>)>,
}

pub(crate) struct KeyedUpdate<'a> {
    pub(crate) state: KeyedState,
    keys: BTreeSet<Key>,
    before: Option<&'a KeyedState>,
    after_readable: bool,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

fn check_keys(rows: &RecordBatch, keys: &RecordBatch, types: &[DataType]) -> Result<()> {
    if rows.num_rows() != keys.num_rows()
        || keys
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type())
            .ne(types.iter())
    {
        return Err(invalid("indexed rows/key lengths or key types differ"));
    }
    // Apply existing limits before grouping/take allocates more Arrow buffers.
    Bag::from_batch(rows)?;
    Bag::from_batch(keys)?;
    Ok(())
}

fn buckets(rows: &RecordBatch, keys: &RecordBatch) -> Result<BTreeMap<Key, Arc<Bucket>>> {
    let mut indices: BTreeMap<Key, Vec<u32>> = BTreeMap::new();
    for (i, key) in encode_keys(keys)?.into_iter().enumerate() {
        indices
            .entry(key)
            .or_default()
            .push(u32::try_from(i).map_err(|_| Error::Overflow)?);
    }
    indices
        .into_iter()
        .map(|(key, indices)| {
            let indices = UInt32Array::from(indices);
            let columns = rows
                .columns()
                .iter()
                .map(|c| take(c.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let batch = RecordBatch::try_new(rows.schema(), columns)?;
            Ok((
                key,
                Arc::new(Bucket {
                    bag: Bag::from_batch(&batch)?,
                    batch,
                }),
            ))
        })
        .collect()
}

fn merge(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    let (rows, bytes) = batches
        .iter()
        .try_fold((0usize, 0usize), |(rows, bytes), b| {
            Ok::<_, Error>((
                rows.checked_add(b.num_rows()).ok_or(Error::Overflow)?,
                bytes
                    .checked_add(b.get_array_memory_size())
                    .ok_or(Error::Overflow)?,
            ))
        })?;
    if rows > MAX_MATERIALIZED_ROWS || bytes > MAX_MATERIALIZED_BYTES {
        return Err(invalid(
            "affected-key snapshot exceeds materialization budget",
        ));
    }
    Ok(concat_batches(schema, &batches)?)
}

impl KeyedState {
    pub(crate) fn new(
        inputs: &[RecordBatch],
        keys: &[RecordBatch],
        output: Option<(&RecordBatch, &RecordBatch)>,
    ) -> Result<Self> {
        if inputs.is_empty() || inputs.len() != keys.len() || keys[0].num_columns() == 0 {
            return Err(invalid("invalid indexed input arity"));
        }
        let key_types = keys[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let mut indexes = vec![];
        for (rows, keys) in inputs.iter().zip(keys) {
            check_keys(rows, keys, &key_types)?;
            indexes.push(buckets(rows, keys)?);
        }
        let outputs = if let Some((rows, keys)) = output {
            check_keys(rows, keys, &key_types)?;
            let outputs = partition(rows, keys)?
                .into_iter()
                .map(|(k, v)| (k, Arc::new(v)))
                .collect::<BTreeMap<_, _>>();
            if indexes.len() != 1 || indexes[0].keys().ne(outputs.keys()) {
                return Err(invalid(
                    "window output partitions differ from input partitions",
                ));
            }
            Some((rows.schema(), outputs))
        } else {
            None
        };
        Ok(Self {
            schemas: inputs.iter().map(RecordBatch::schema).collect(),
            key_types,
            inputs: indexes,
            outputs,
        })
    }

    pub(crate) fn totals(&self) -> Vec<usize> {
        self.inputs.iter().map(BTreeMap::len).collect()
    }

    pub(crate) fn prepare(
        &self,
        positives: &[RecordBatch],
        positive_keys: &[RecordBatch],
        negatives: &[RecordBatch],
        negative_keys: &[RecordBatch],
        need_before: bool,
        need_after: bool,
    ) -> Result<KeyedUpdate<'_>> {
        let n = self.inputs.len();
        if [
            positives.len(),
            positive_keys.len(),
            negatives.len(),
            negative_keys.len(),
        ]
        .iter()
        .any(|len| *len != n)
        {
            return Err(invalid("indexed delta arity differs"));
        }
        let mut next = self.clone();
        let mut touched = BTreeSet::new();
        for port in 0..n {
            for (rows, keys) in [
                (&positives[port], &positive_keys[port]),
                (&negatives[port], &negative_keys[port]),
            ] {
                if rows.schema() != self.schemas[port] {
                    return Err(invalid("indexed input schema changed"));
                }
                check_keys(rows, keys, &self.key_types)?;
            }
            let mut negative = partition(&negatives[port], &negative_keys[port])?;
            let mut changes = BTreeMap::new();
            for (key, pos) in partition(&positives[port], &positive_keys[port])? {
                let delta = match negative.remove(&key) {
                    Some(neg) => pos.difference(&neg)?,
                    None => pos,
                };
                if !delta.is_empty() {
                    changes.insert(key, delta);
                }
            }
            let empty = Bag::empty(self.schemas[port].clone());
            for (key, neg) in negative {
                changes.insert(key, empty.difference(&neg)?);
            }
            for (key, delta) in changes {
                touched.insert(key.clone());
                let before = self.inputs[port].get(&key).map_or(&empty, |b| &b.bag);
                let after = before.add(&delta)?;
                after.validate_relation()?;
                if after.is_empty() {
                    next.inputs[port].remove(&key);
                } else {
                    let batch = after.to_batch()?;
                    next.inputs[port].insert(key, Arc::new(Bucket { bag: after, batch }));
                }
            }
        }
        Ok(KeyedUpdate {
            state: next,
            keys: touched,
            // Keep immutable buckets, not eagerly materialized snapshot inputs.
            // Physical lookups alone read the requested versions and ports.
            before: need_before.then_some(self),
            after_readable: need_after,
        })
    }
}

impl KeyedUpdate<'_> {
    pub(crate) fn touched(&self) -> usize {
        self.keys.len()
    }
    /// Materialize exactly one requested port over the affected-key closure.
    pub(crate) fn lookup_input(
        &self,
        port: usize,
        version: SnapshotVersion,
    ) -> Result<RecordBatch> {
        let state = match version {
            SnapshotVersion::Before => self
                .before
                .as_ref()
                .ok_or_else(|| invalid("Before index is not bound"))?,
            SnapshotVersion::After if self.after_readable => &self.state,
            SnapshotVersion::After => return Err(invalid("After index is not bound")),
        };
        let index = state
            .inputs
            .get(port)
            .ok_or_else(|| invalid("index lookup port is invalid"))?;
        let schema = state
            .schemas
            .get(port)
            .ok_or_else(|| invalid("index schema port is invalid"))?;
        merge(
            schema,
            self.keys
                .iter()
                .filter_map(|key| index.get(key).map(|bucket| bucket.batch.clone()))
                .collect(),
        )
    }

    /// Read actual committed output buckets when the physical plan requests them.
    pub(crate) fn lookup_output(&self) -> Result<RecordBatch> {
        let before = self
            .before
            .as_ref()
            .ok_or_else(|| invalid("Before output is not bound"))?;
        let (schema, outputs) = before
            .outputs
            .as_ref()
            .ok_or_else(|| invalid("missing committed output index"))?;
        let batches = self
            .keys
            .iter()
            .filter_map(|key| outputs.get(key))
            .map(|bag| bag.to_batch())
            .collect::<Result<Vec<_>>>()?;
        merge(schema, batches)
    }

    /// Follow the executed custom program, rather than silently replacing its output.
    pub(crate) fn apply_window_delta(
        &mut self,
        pos: &RecordBatch,
        pos_keys: &RecordBatch,
        neg: &RecordBatch,
        neg_keys: &RecordBatch,
    ) -> Result<()> {
        let Some((schema, outputs)) = &mut self.state.outputs else {
            return Err(invalid("missing window output index"));
        };
        if pos.schema() != *schema || neg.schema() != *schema {
            return Err(invalid("window output schema changed"));
        }
        check_keys(pos, pos_keys, &self.state.key_types)?;
        check_keys(neg, neg_keys, &self.state.key_types)?;
        let positive = partition(pos, pos_keys)?;
        let negative = partition(neg, neg_keys)?;
        let keys = positive
            .keys()
            .chain(negative.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let empty = Bag::empty(schema.clone());
        for key in keys {
            if !self.keys.contains(&key) {
                return Err(invalid("window delta escapes affected partitions"));
            }
            let old = outputs.get(&key).map_or(&empty, |b| b.as_ref());
            let after = old
                .add(positive.get(&key).unwrap_or(&empty))?
                .difference(negative.get(&key).unwrap_or(&empty))?;
            after.validate_relation()?;
            if after.is_empty() {
                outputs.remove(&key);
            } else {
                outputs.insert(key, Arc::new(after));
            }
        }
        Ok(())
    }
}
