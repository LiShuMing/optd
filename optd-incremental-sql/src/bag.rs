//! Exact, typed multisets used at SQL operator boundaries.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;

use crate::{Error, Result};

mod range;

/// Maximum rows in one local input or expanded bag materialization.
///
/// This reference backend rejects larger expansions before allocating Arrow
/// output, so a corrupt or accidental large weight cannot exhaust host memory.
pub const MAX_MATERIALIZED_ROWS: usize = 1_000_000;
/// Conservative scalar-size accounting also bounds wide or variable-width rows.
pub(crate) const MAX_MATERIALIZED_BYTES: usize = 256 * 1024 * 1024;

/// Bound each temporary lookup-encoding batch, independent of signed delta size.
const MAX_KEY_ENCODING_ROWS: usize = 1024;

pub(crate) type Entry<'a> = (&'a Vec<ScalarValue>, i64);

/// A schema-bound signed multiset, retaining DataFusion scalar identity exactly.
///
/// Zero weights are removed. Negative weights are valid changes, but cannot be
/// materialized as a relation until [`Self::validate_relation`] succeeds. Float
/// identity follows `ScalarValue`: signed zeros and NaN payloads remain distinct.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Bag {
    schema: SchemaRef,
    rows: Arc<HashMap<Vec<ScalarValue>, i64>>,
}

/// One exact complete row associated with its Arrow sort encoding.
pub(crate) struct KeyedBagRow<'a> {
    pub(crate) key: Vec<u8>,
    pub(crate) values: &'a [ScalarValue],
    pub(crate) weight: i64,
}

/// Temporary encoding volume, separate from relation cache materialization.
pub(crate) struct EncodedBagRows<'a> {
    pub(crate) rows: Vec<KeyedBagRow<'a>>,
    pub(crate) array_bytes: usize,
}

/// Expanded scalar estimates used by the existing materialization safety contract.
pub(crate) struct MaterializationVolume {
    pub(crate) rows: usize,
    pub(crate) scalar_bytes: usize,
}

/// Canonical distinct rows and exact weighted logical validity from that encoding.
pub(crate) struct DistinctRows {
    pub(crate) batch: RecordBatch,
    pub(crate) null_counts: Vec<usize>,
}

impl Bag {
    /// Copy a trusted same-schema subset of already consolidated entries.
    /// Callers must not pass duplicate complete rows; reject rather than overwrite.
    pub(crate) fn from_unique_entries<'a>(
        schema: SchemaRef,
        entries: impl IntoIterator<Item = (&'a [ScalarValue], i64)>,
    ) -> Result<Self> {
        let mut rows = HashMap::new();
        for (row, weight) in entries {
            if weight == 0 || row.len() != schema.fields().len() {
                return Err(Error::Invalid("invalid consolidated row entry".into()));
            }
            rows.try_reserve(1).map_err(allocation_error)?;
            if rows.insert(clone_row(row)?, weight).is_some() {
                return Err(Error::Invalid("duplicate consolidated row entry".into()));
            }
        }
        Ok(Self {
            schema,
            rows: Arc::new(rows),
        })
    }

    /// Split an ordered relation into bounded distinct-entry subsets.
    /// A singleton uses the existing no-sort path for otherwise unorderable types.
    pub(crate) fn split(&self, bound: usize) -> Result<Vec<Self>> {
        if bound == 0 {
            return Err(Error::Invalid("zero table block bound".into()));
        }
        if self.is_empty() {
            return Ok(Vec::new());
        }
        if self.len() <= bound {
            return Ok(vec![self.clone()]);
        }
        self.sorted_entries()?
            .chunks(bound)
            .map(|entries| {
                Self::from_unique_entries(
                    self.schema(),
                    entries
                        .iter()
                        .map(|(row, weight)| (row.as_slice(), *weight)),
                )
            })
            .collect()
    }

    /// Encode signed entries for lookup with the table's shared converter.
    /// Final relation leaves validate scalar identity on encoding ties: old/new
    /// images may tie even when their final singleton relation remains admitted.
    pub(crate) fn keyed_rows(&self, converter: &RowConverter) -> Result<EncodedBagRows<'_>> {
        let entries = self
            .rows
            .iter()
            .map(|(row, weight)| (row, *weight))
            .collect::<Vec<_>>();
        // A signed two-image UPDATE may contain more entries/bytes than either
        // valid endpoint. Bound each encoding batch, not the combined delta.
        let mut rows = Vec::new();
        rows.try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        let mut array_bytes = 0usize;
        let mut start = 0usize;
        while start < entries.len() {
            let mut end = start;
            let mut bytes = 0usize;
            while end < entries.len() && end - start < MAX_KEY_ENCODING_ROWS {
                let next = bytes
                    .checked_add(scalar_bytes(entries[end].0)?)
                    .ok_or(Error::Overflow)?;
                if end > start && next > MAX_MATERIALIZED_BYTES {
                    break;
                }
                bytes = next;
                end += 1;
            }
            let batch = materialize(self.schema(), &entries[start..end], false)?;
            array_bytes = array_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or(Error::Overflow)?;
            let encoded = converter.convert_columns(batch.columns())?;
            for (i, (row, weight)) in entries[start..end].iter().enumerate() {
                rows.push(KeyedBagRow {
                    key: encoded.row(i).as_ref().to_vec(),
                    values: row,
                    weight: *weight,
                });
            }
            start = end;
        }
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        Ok(EncodedBagRows { rows, array_bytes })
    }

    /// Compute the same expanded row/scalar estimates as to_batch, without expansion.
    pub(crate) fn materialization_volume(&self) -> Result<MaterializationVolume> {
        let mut rows = 0usize;
        let mut bytes = 0usize;
        self.validate_relation()?;
        for (row, weight) in self.rows.iter() {
            let repeats = usize::try_from(*weight).map_err(|_| Error::Overflow)?;
            rows = rows.checked_add(repeats).ok_or(Error::Overflow)?;
            bytes = bytes
                .checked_add(
                    scalar_bytes(row)?
                        .checked_mul(repeats)
                        .ok_or(Error::Overflow)?,
                )
                .ok_or(Error::Overflow)?;
        }
        check_materialization_volume(rows, bytes)?;
        Ok(MaterializationVolume {
            rows,
            scalar_bytes: bytes,
        })
    }

    /// Construct an empty relation without losing its column types or metadata.
    pub(crate) fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            rows: Arc::new(HashMap::new()),
        }
    }

    /// Collect a positive Arrow relation, consolidating duplicate complete rows.
    pub(crate) fn from_batch(batch: &RecordBatch) -> Result<Self> {
        Self::collect(batch, |_| 1)
    }

    /// Collect signed changes, requiring exactly one weight per input row.
    pub(crate) fn from_weighted(batch: &RecordBatch, weights: &[i64]) -> Result<Self> {
        if batch.num_rows() != weights.len() {
            return Err(Error::Invalid(format!(
                "weight count {} differs from batch row count {}",
                weights.len(),
                batch.num_rows()
            )));
        }
        Self::collect(batch, |row| weights[row])
    }

    fn collect(batch: &RecordBatch, weight: impl Fn(usize) -> i64) -> Result<Self> {
        check_row_limit(batch.num_rows())?;
        if retained_input_bytes(batch)? > MAX_MATERIALIZED_BYTES {
            return Err(Error::Invalid(format!(
                "local bag input exceeds the {MAX_MATERIALIZED_BYTES}-byte safety limit"
            )));
        }
        let mut bag = Self::empty(batch.schema());
        Arc::make_mut(&mut bag.rows)
            .try_reserve(batch.num_rows())
            .map_err(allocation_error)?;
        let mut stored_bytes = 0_usize;
        for index in 0..batch.num_rows() {
            let weight = weight(index);
            if weight == 0 {
                continue;
            }
            let mut row = Vec::new();
            row.try_reserve_exact(batch.num_columns())
                .map_err(allocation_error)?;
            for column in batch.columns() {
                row.push(ScalarValue::try_from_array(column.as_ref(), index)?);
            }
            let previous = bag.rows.get(&row).copied().unwrap_or_default();
            let next = previous.checked_add(weight).ok_or(Error::Overflow)?;
            if next == 0 {
                if let Some((stored_row, _)) = bag.rows.get_key_value(&row) {
                    stored_bytes = stored_bytes
                        .checked_sub(scalar_bytes(stored_row)?)
                        .ok_or(Error::Overflow)?;
                }
                Arc::make_mut(&mut bag.rows).remove(&row);
            } else {
                if previous == 0 {
                    stored_bytes = stored_bytes
                        .checked_add(scalar_bytes(&row)?)
                        .ok_or(Error::Overflow)?;
                    check_byte_limit(stored_bytes)?;
                }
                Arc::make_mut(&mut bag.rows).insert(row, next);
            }
        }
        Ok(bag)
    }

    /// Return the complete schema; equality includes names, nullability and metadata.
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Whether consolidation removed every row.
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Number of distinct complete rows with a nonzero weight.
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    /// Count expanded relation rows, rejecting negative or overflowing totals.
    pub(crate) fn total(&self) -> Result<usize> {
        self.validate_relation()?;
        self.rows.values().try_fold(0_usize, |total, weight| {
            total
                .checked_add(usize::try_from(*weight).map_err(|_| Error::Overflow)?)
                .ok_or(Error::Overflow)
        })
    }

    /// Add two same-schema signed bags without modifying either operand.
    pub(crate) fn add(&self, other: &Self) -> Result<Self> {
        if self.schema == other.schema && other.is_empty() {
            return Ok(self.clone());
        }
        self.combine(other, i64::checked_add)
    }

    /// Borrow exact complete rows without sorting, encoding or expanding weights.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&[ScalarValue], i64)> {
        self.rows
            .iter()
            .map(|(row, weight)| (row.as_slice(), *weight))
    }

    /// Probe one exact row; hash collisions are resolved by ScalarValue equality.
    pub(crate) fn weight(&self, row: &[ScalarValue]) -> i64 {
        self.rows.get(row).copied().unwrap_or_default()
    }

    /// Subtract directly with checked arithmetic, including `MIN - MIN == 0`.
    pub(crate) fn difference(&self, other: &Self) -> Result<Self> {
        if self.schema == other.schema {
            if Arc::ptr_eq(&self.rows, &other.rows) {
                return Ok(Self::empty(self.schema()));
            }
            if other.is_empty() {
                return Ok(self.clone());
            }
        }
        self.combine(other, i64::checked_sub)
    }

    fn combine(&self, other: &Self, operation: fn(i64, i64) -> Option<i64>) -> Result<Self> {
        if self.schema != other.schema {
            return Err(Error::Invalid("bag schemas differ".into()));
        }
        let capacity = self
            .rows
            .len()
            .checked_add(other.rows.len())
            .ok_or(Error::Overflow)?;
        let mut rows = HashMap::new();
        rows.try_reserve(capacity).map_err(allocation_error)?;
        for (row, weight) in self.rows.iter() {
            rows.insert(clone_row(row)?, *weight);
        }
        for (row, weight) in other.rows.iter() {
            let previous = rows.get(row).copied().unwrap_or_default();
            let next = operation(previous, *weight).ok_or(Error::Overflow)?;
            if next == 0 {
                rows.remove(row);
            } else {
                rows.insert(clone_row(row)?, next);
            }
        }
        Ok(Self {
            schema: self.schema(),
            rows: Arc::new(rows),
        })
    }

    /// Reverse every change, rejecting the unrepresentable negation of `i64::MIN`.
    #[cfg(test)]
    pub(crate) fn negate(&self) -> Result<Self> {
        let mut rows = HashMap::new();
        rows.try_reserve(self.len()).map_err(allocation_error)?;
        for (row, weight) in self.rows.iter() {
            rows.insert(
                clone_row(row)?,
                weight.checked_neg().ok_or(Error::Overflow)?,
            );
        }
        Ok(Self {
            schema: self.schema(),
            rows: Arc::new(rows),
        })
    }

    /// Ensure a candidate relation contains no absent-row deletion.
    pub(crate) fn validate_relation(&self) -> Result<()> {
        if self.rows.values().any(|weight| *weight < 0) {
            return Err(Error::Invalid(
                "relation contains a negative multiplicity (deletion of an absent row)".into(),
            ));
        }
        Ok(())
    }

    /// Preflight complete positive bags without sorting or allocating Arrow output.
    /// Preparation retains its early resource rejection even when physical
    /// lookups defer actual materialization until execution.
    pub(crate) fn check_materialization<'a>(
        bags: impl IntoIterator<Item = &'a Self>,
    ) -> Result<usize> {
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for bag in bags {
            bag.validate_relation()?;
            for (row, weight) in bag.rows.iter() {
                let repeats = usize::try_from(*weight).map_err(|_| Error::Overflow)?;
                rows = rows.checked_add(repeats).ok_or(Error::Overflow)?;
                check_row_limit(rows)?;
                bytes = bytes
                    .checked_add(
                        scalar_bytes(row)?
                            .checked_mul(repeats)
                            .ok_or(Error::Overflow)?,
                    )
                    .ok_or(Error::Overflow)?;
            }
        }
        check_byte_limit(bytes)?;
        Ok(rows)
    }

    /// Expand positive multiplicities in deterministic complete-row order.
    pub(crate) fn to_batch(&self) -> Result<RecordBatch> {
        check_row_limit(self.total()?)?;
        materialize(self.schema(), &self.sorted_entries()?, true)
    }

    /// Encode one distinct row per change and append the signed Int64 weight.
    pub(crate) fn weighted_batch(&self) -> Result<RecordBatch> {
        let entries = self.sorted_entries()?;
        let batch = materialize(self.schema(), &entries, false)?;
        let mut columns = batch.columns().to_vec();
        columns.try_reserve(1).map_err(allocation_error)?;
        let mut weights = Vec::new();
        weights
            .try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        weights.extend(entries.iter().map(|(_, weight)| *weight));
        columns.push(Arc::new(Int64Array::from(weights)));
        let mut fields = self.schema.fields().to_vec();
        fields.try_reserve(1).map_err(allocation_error)?;
        fields.push(Arc::new(Field::new(
            "__optd_weight",
            DataType::Int64,
            false,
        )));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));
        batch_with_rows(schema, columns, entries.len())
    }

    /// Expand insertions and retractions separately as positive Arrow relations.
    pub(crate) fn signed_parts(&self) -> Result<(RecordBatch, RecordBatch)> {
        let entries = self.sorted_entries()?;
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        positive
            .try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        negative
            .try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        for (row, weight) in entries {
            if weight > 0 {
                positive.push((row, weight));
            } else {
                negative.push((row, weight.checked_neg().ok_or(Error::Overflow)?));
            }
        }
        Ok((
            materialize(self.schema(), &positive, true)?,
            materialize(self.schema(), &negative, true)?,
        ))
    }

    /// Consolidate independently evaluated positive and negative operator results.
    pub(crate) fn from_parts(positive: &RecordBatch, negative: &RecordBatch) -> Result<Self> {
        Self::from_batch(positive)?.difference(&Self::from_batch(negative)?)
    }

    /// Borrow canonical distinct entries without expanding multiplicities.
    /// Leaf callers bound cardinality before allocating this ordered reference vector.
    pub(crate) fn sorted_entries(&self) -> Result<Vec<Entry<'_>>> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.len())
            .map_err(allocation_error)?;
        entries.extend(self.rows.iter().map(|(row, weight)| (row, *weight)));
        if entries.len() < 2 || self.schema.fields().is_empty() {
            return Ok(entries);
        }
        let batch = materialize(self.schema(), &entries, false)?;
        let fields = self
            .schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields)?;
        let encoded = converter.convert_columns(batch.columns())?;
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        indices.extend(0..entries.len());
        indices.sort_unstable_by(|left, right| encoded.row(*left).cmp(&encoded.row(*right)));
        // Equal sort encodings must identify equal scalar rows. Reject an
        // unsupported encoding instead of exposing HashMap order on a tie.
        for adjacent in indices.windows(2) {
            let left = adjacent[0];
            let right = adjacent[1];
            if encoded.row(left) == encoded.row(right) && entries[left].0 != entries[right].0 {
                return Err(Error::Invalid(
                    "Arrow row ordering does not preserve this scalar identity".into(),
                ));
            }
        }
        let mut sorted = Vec::new();
        sorted
            .try_reserve_exact(entries.len())
            .map_err(allocation_error)?;
        sorted.extend(indices.into_iter().map(|index| entries[index]));
        Ok(sorted)
    }
}

/// Materialize only after checking both expansion arithmetic and an explicit budget.
fn materialize(schema: SchemaRef, entries: &[Entry<'_>], expand: bool) -> Result<RecordBatch> {
    let mut count = 0_usize;
    let mut estimated_bytes = 0_usize;
    for (row, weight) in entries {
        let repeats = if expand {
            usize::try_from(*weight).map_err(|_| Error::Overflow)?
        } else {
            1
        };
        count = count.checked_add(repeats).ok_or(Error::Overflow)?;
        check_row_limit(count)?;
        let row_bytes = scalar_bytes(row)?;
        estimated_bytes = estimated_bytes
            .checked_add(row_bytes.checked_mul(repeats).ok_or(Error::Overflow)?)
            .ok_or(Error::Overflow)?;
    }
    check_byte_limit(estimated_bytes)?;
    let mut columns = Vec::new();
    columns
        .try_reserve_exact(schema.fields().len())
        .map_err(allocation_error)?;
    for (column, field) in schema.fields().iter().enumerate() {
        if count == 0 {
            columns.push(new_empty_array(field.data_type()));
            continue;
        }
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(allocation_error)?;
        for (row, weight) in entries {
            let repeats = if expand {
                usize::try_from(*weight).map_err(|_| Error::Overflow)?
            } else {
                1
            };
            values.extend(std::iter::repeat_n(row[column].clone(), repeats));
        }
        columns.push(ScalarValue::iter_to_array(values)?);
    }
    batch_with_rows(schema, columns, count)
}

/// Arrow requires an explicit row count for nonempty zero-column relations.
fn batch_with_rows(schema: SchemaRef, columns: Vec<ArrayRef>, count: usize) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(count)),
    )?)
}

/// Preserve global resource limits when an immutable table has several leaves.
pub(crate) fn check_materialization_volume(rows: usize, bytes: usize) -> Result<()> {
    check_row_limit(rows)?;
    check_byte_limit(bytes)
}

fn check_row_limit(count: usize) -> Result<()> {
    if count > MAX_MATERIALIZED_ROWS {
        return Err(Error::Invalid(format!(
            "local bag materialization requires {count} rows, exceeding the {MAX_MATERIALIZED_ROWS}-row safety limit"
        )));
    }
    Ok(())
}

fn check_byte_limit(estimated_bytes: usize) -> Result<()> {
    if estimated_bytes > MAX_MATERIALIZED_BYTES {
        return Err(Error::Invalid(format!(
            "local bag materialization estimates {estimated_bytes} scalar bytes, exceeding the {MAX_MATERIALIZED_BYTES}-byte safety limit"
        )));
    }
    Ok(())
}

fn scalar_bytes(row: &[ScalarValue]) -> Result<usize> {
    row.iter().try_fold(0_usize, |size, value| {
        size.checked_add(value.size()).ok_or(Error::Overflow)
    })
}

fn allocation_error(error: std::collections::TryReserveError) -> Error {
    Error::Invalid(format!("cannot allocate local typed bag: {error}"))
}

fn clone_row(row: &[ScalarValue]) -> Result<Vec<ScalarValue>> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(row.len())
        .map_err(allocation_error)?;
    cloned.extend_from_slice(row);
    Ok(cloned)
}

/// Count owned buffer allocations once even when IPC columns share a backing
/// message. Array memory-size sums count the same capacity for every slice.
fn retained_input_bytes(batch: &RecordBatch) -> Result<usize> {
    use datafusion::arrow::array::ArrayData;
    use datafusion::arrow::buffer::Buffer;
    use std::collections::BTreeMap;

    fn visit(data: &ArrayData, allocations: &mut BTreeMap<usize, usize>) -> Result<usize> {
        let mut metadata = std::mem::size_of::<ArrayData>()
            .checked_add(
                data.buffers()
                    .len()
                    .checked_mul(std::mem::size_of::<Buffer>())
                    .ok_or(Error::Overflow)?,
            )
            .ok_or(Error::Overflow)?;
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            let bytes = buffer.capacity().max(
                buffer
                    .ptr_offset()
                    .checked_add(buffer.len())
                    .ok_or(Error::Overflow)?,
            );
            let entry = allocations
                .entry(buffer.data_ptr().as_ptr() as usize)
                .or_default();
            *entry = (*entry).max(bytes);
        }
        for child in data.child_data() {
            metadata = metadata
                .checked_add(visit(child, allocations)?)
                .ok_or(Error::Overflow)?;
        }
        Ok(metadata)
    }
    let mut allocations = BTreeMap::new();
    let mut bytes = 0usize;
    for column in batch.columns() {
        bytes = bytes
            .checked_add(visit(&column.to_data(), &mut allocations)?)
            .ok_or(Error::Overflow)?;
    }
    allocations
        .values()
        .try_fold(bytes, |sum, n| sum.checked_add(*n).ok_or(Error::Overflow))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, StringArray,
    };

    fn ints(values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn immutable_clones_share_row_storage_and_changes_leave_before_intact() -> Result<()> {
        let before = Bag::from_batch(&ints(vec![Some(1), Some(2)]))?;
        let candidate = before.clone();
        assert!(Arc::ptr_eq(&before.rows, &candidate.rows));
        let delta = Bag::from_weighted(&ints(vec![Some(1), Some(3)]), &[-1, 1])?;
        let after = candidate.add(&delta)?;
        assert_eq!(before, Bag::from_batch(&ints(vec![Some(1), Some(2)]))?);
        assert_eq!(after, Bag::from_batch(&ints(vec![Some(2), Some(3)]))?);
        assert!(!Arc::ptr_eq(&before.rows, &after.rows));
        Ok(())
    }

    #[test]
    fn signed_changes_consolidate_and_roundtrip() -> Result<()> {
        let batch = ints(vec![Some(7), Some(7), None, Some(9), Some(9)]);
        let changes = Bag::from_weighted(&batch, &[3, -1, -2, 4, -4])?;
        assert_eq!(changes.len(), 2);
        assert!(changes.validate_relation().is_err());
        let (positive, negative) = changes.signed_parts()?;
        assert_eq!(positive.num_rows(), 2);
        assert_eq!(negative.num_rows(), 2);
        assert_eq!(Bag::from_parts(&positive, &negative)?, changes);
        assert!(changes.add(&changes.negate()?)?.is_empty());
        let weighted = changes.weighted_batch()?;
        assert_eq!(weighted.num_rows(), 2);
        assert_eq!(weighted.schema().field(1).name(), "__optd_weight");
        assert_eq!(weighted.schema().field(1).data_type(), &DataType::Int64);
        Ok(())
    }

    #[test]
    fn invalid_deletion_and_overflow_do_not_mutate_inputs() -> Result<()> {
        let batch = ints(vec![Some(1)]);
        let original = Bag::from_batch(&batch)?;
        let deletion = Bag::from_weighted(&batch, &[-2])?;
        assert!(original.add(&deletion)?.validate_relation().is_err());
        assert_eq!(original.total()?, 1);
        let maximum = Bag::from_weighted(&batch, &[i64::MAX])?;
        assert!(matches!(maximum.add(&original), Err(Error::Overflow)));
        let minimum = Bag::from_weighted(&batch, &[i64::MIN])?;
        assert!(matches!(minimum.negate(), Err(Error::Overflow)));
        assert!(minimum.difference(&minimum)?.is_empty());
        assert!(maximum.to_batch().is_err());
        assert_eq!(maximum.weighted_batch()?.num_rows(), 1);
        assert!(Bag::from_weighted(&batch, &[]).is_err());
        assert!(Bag::from_weighted(&ints(vec![Some(1), Some(1)]), &[i64::MAX, 1]).is_err());
        let excessive_total =
            Bag::from_weighted(&ints(vec![Some(1), Some(2), Some(3)]), &[i64::MAX; 3])?;
        assert!(matches!(excessive_total.total(), Err(Error::Overflow)));
        Ok(())
    }

    #[test]
    fn materialization_checks_row_and_byte_budgets_before_expansion() -> Result<()> {
        let unit = batch_with_rows(Arc::new(Schema::empty()), vec![], 1)?;
        let count = i64::try_from(MAX_MATERIALIZED_ROWS + 1).map_err(|_| Error::Overflow)?;
        let too_many = Bag::from_weighted(&unit, &[count])?;
        match too_many.to_batch() {
            Err(Error::Invalid(message)) => assert!(message.contains("1000000-row safety limit")),
            other => panic!("expected the explicit row budget error, got {other:?}"),
        }
        let schema = Arc::new(Schema::new(
            (0..16)
                .map(|index| Field::new(format!("c{index}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let columns = (0..16)
            .map(|_| Arc::new(Int64Array::from(vec![7])) as ArrayRef)
            .collect();
        let wide = RecordBatch::try_new(schema, columns)?;
        let repeats = i64::try_from(MAX_MATERIALIZED_ROWS).map_err(|_| Error::Overflow)?;
        match Bag::from_weighted(&wide, &[repeats])?.to_batch() {
            Err(Error::Invalid(message)) => assert!(message.contains("-byte safety limit")),
            other => panic!("expected the explicit scalar budget error, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn zero_column_relations_keep_multiplicity() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batch = batch_with_rows(Arc::clone(&schema), vec![], 3)?;
        let bag = Bag::from_batch(&batch)?;
        assert_eq!(bag.len(), 1);
        assert_eq!(bag.total()?, 3);
        assert_eq!(bag.to_batch()?.num_rows(), 3);
        assert_eq!(bag.weighted_batch()?.num_rows(), 1);
        let empty = Bag::empty(schema);
        assert_eq!(empty.to_batch()?.num_rows(), 0);
        let (positive, negative) = bag.negate()?.signed_parts()?;
        assert_eq!(positive.num_rows(), 0);
        assert_eq!(negative.num_rows(), 3);
        assert_eq!(Bag::from_parts(&positive, &negative)?, bag.negate()?);
        Ok(())
    }

    #[test]
    fn exact_decimal_date_boolean_null_and_schema_roundtrip() -> Result<()> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("amount", DataType::Decimal128(30, 4), true),
                Field::new("date", DataType::Date32, true),
                Field::new("flag", DataType::Boolean, true),
                Field::new("text", DataType::Utf8, true),
            ],
            HashMap::from([("fixture".into(), "typed".into())]),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(90_071_992_547_409_930_i128), None])
                        .with_precision_and_scale(30, 4)?,
                ),
                Arc::new(Date32Array::from(vec![Some(20_000), None])),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(StringArray::from(vec![Some("日期"), None])),
            ],
        )?;
        let bag = Bag::from_batch(&batch)?;
        assert_eq!(Bag::from_batch(&bag.to_batch()?)?, bag);
        assert_eq!(bag.schema(), schema);
        let empty = Bag::empty(Arc::clone(&schema)).to_batch()?;
        assert_eq!(empty.schema(), schema);
        assert_eq!(empty.num_rows(), 0);
        let other_schema = Arc::new(Schema::new(schema.fields().clone()));
        assert_ne!(Bag::empty(schema), Bag::empty(Arc::clone(&other_schema)));
        assert!(bag.add(&Bag::empty(other_schema)).is_err());
        Ok(())
    }

    #[test]
    fn float_bit_identity_and_output_order_are_stable() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let values = [
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            None,
        ];
        let make = |values: Vec<Option<f64>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float64Array::from(values))],
            )
        };
        let first = Bag::from_batch(&make(values.to_vec())?)?;
        let second = Bag::from_batch(&make(values.into_iter().rev().collect())?)?;
        assert_eq!(first.len(), 5);
        assert_eq!(first, second);
        let output = first.to_batch()?;
        assert_eq!(Bag::from_batch(&output)?, first);
        let scalar_rows = |batch: &RecordBatch| -> Result<Vec<ScalarValue>> {
            (0..batch.num_rows())
                .map(|index| {
                    Ok(ScalarValue::try_from_array(
                        batch.column(0).as_ref(),
                        index,
                    )?)
                })
                .collect()
        };
        assert_eq!(scalar_rows(&output)?, scalar_rows(&second.to_batch()?)?);
        Ok(())
    }
}

#[cfg(test)]
mod allocation_budget_tests {
    use super::*;

    fn reserved_array(capacity: usize) -> Arc<Int64Array> {
        let mut values = Vec::with_capacity(capacity);
        values.push(7);
        Arc::new(Int64Array::from(values))
    }

    #[test]
    fn shared_arrow_allocations_count_once_and_oversized_allocations_still_reject() {
        let array = reserved_array(MAX_MATERIALIZED_BYTES / 8 / 4);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(
                (0..8)
                    .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
                    .collect::<Vec<_>>(),
            )),
            vec![array as ArrayRef; 8],
        )
        .unwrap();
        assert!(batch.get_array_memory_size() > MAX_MATERIALIZED_BYTES);
        assert!(retained_input_bytes(&batch).unwrap() < MAX_MATERIALIZED_BYTES);
        assert_eq!(Bag::from_batch(&batch).unwrap().total().unwrap(), 1);
        let oversized = RecordBatch::try_from_iter(vec![(
            "v",
            reserved_array(MAX_MATERIALIZED_BYTES / 8 + 1) as ArrayRef,
        )])
        .unwrap();
        assert!(retained_input_bytes(&oversized).unwrap() > MAX_MATERIALIZED_BYTES);
        assert!(Bag::from_batch(&oversized).is_err());
    }
}
