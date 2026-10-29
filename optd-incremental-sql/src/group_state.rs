//! Derived, transaction-local indexes for recomputing only changed SQL groups.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::row::{RowConverter, SortField};

use crate::bag::Bag;
use crate::{Error, Result};

type GroupKey = Vec<u8>;

/// Caller proof that the fixed SQL context uses binary string equality.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyPolicy {
    pub(crate) binary_strings: bool,
}

#[derive(Debug)]
struct GroupBucket {
    input: Bag,
    output: Bag,
}

/// A derived index; unchanged buckets are shared with prepared candidates.
#[derive(Clone, Debug)]
pub(crate) struct GroupState {
    input_schema: SchemaRef,
    key_schema: SchemaRef,
    output_schema: SchemaRef,
    groups: BTreeMap<GroupKey, Arc<GroupBucket>>,
}

/// A candidate with materialized input only for groups whose multiset changed.
#[derive(Debug)]
pub(crate) struct GroupUpdate {
    state: GroupState,
    touched: BTreeMap<GroupKey, Bag>,
    affected_input: RecordBatch,
}

impl GroupState {
    /// Build an index, returning None only for a known unsupported key policy.
    pub(crate) fn try_new(
        input: &RecordBatch,
        projected_keys: &RecordBatch,
        aggregate_output: &RecordBatch,
        policy: KeyPolicy,
    ) -> Result<Option<Self>> {
        if projected_keys.num_columns() == 0
            || projected_keys
                .schema()
                .fields()
                .iter()
                .any(|field| !supported_key(field.data_type(), policy))
        {
            return Ok(None);
        }
        check_pair(input, projected_keys)?;
        let inputs = partition(input, projected_keys)?;
        let mut outputs = output_groups(aggregate_output, &projected_keys.schema())?;
        let mut groups = BTreeMap::new();
        for (key, input) in inputs {
            let output = outputs.remove(&key).ok_or_else(|| {
                Error::Invalid("aggregate output omitted an existing group".into())
            })?;
            groups.insert(key, Arc::new(GroupBucket { input, output }));
        }
        if !outputs.is_empty() {
            return Err(Error::Invalid(
                "aggregate output contains an unknown group".into(),
            ));
        }
        Ok(Some(Self {
            input_schema: input.schema(),
            key_schema: projected_keys.schema(),
            output_schema: aggregate_output.schema(),
            groups,
        }))
    }

    /// Prepare a signed, atomic input change without mutating the installed index.
    pub(crate) fn prepare(
        &self,
        positive_rows: &RecordBatch,
        positive_keys: &RecordBatch,
        negative_rows: &RecordBatch,
        negative_keys: &RecordBatch,
    ) -> Result<GroupUpdate> {
        for (rows, keys) in [
            (positive_rows, positive_keys),
            (negative_rows, negative_keys),
        ] {
            if rows.schema() != self.input_schema || keys.schema() != self.key_schema {
                return Err(Error::Invalid("group input/key schemas changed".into()));
            }
            check_pair(rows, keys)?;
        }
        let positive = partition(positive_rows, positive_keys)?;
        let mut negative = partition(negative_rows, negative_keys)?;
        let mut changes = BTreeMap::new();
        for (key, rows) in positive {
            let delta = if let Some(retracted) = negative.remove(&key) {
                rows.difference(&retracted)?
            } else {
                rows
            };
            if !delta.is_empty() {
                changes.insert(key, delta);
            }
        }
        let empty = Bag::empty(Arc::clone(&self.input_schema));
        for (key, rows) in negative {
            changes.insert(key, empty.difference(&rows)?);
        }
        let mut touched = BTreeMap::new();
        for (key, delta) in changes {
            let before = self.groups.get(&key).map_or(&empty, |bucket| &bucket.input);
            let after = before.add(&delta)?;
            after.validate_relation()?;
            touched.insert(key, after);
        }
        // Bag performs the aggregate row/byte checks before Arrow allocation.
        // Balanced merging avoids repeatedly copying an ever-growing prefix.
        let affected_input = merge_bags(
            Arc::clone(&self.input_schema),
            &touched.values().collect::<Vec<_>>(),
        )?
        .to_batch()?;
        Ok(GroupUpdate {
            state: self.clone(),
            touched,
            affected_input,
        })
    }

    /// Count installed SQL groups, including NULL-key and all-NULL-value groups.
    pub(crate) fn total_groups(&self) -> usize {
        self.groups.len()
    }
}

impl GroupUpdate {
    /// Return only surviving rows of touched groups, retaining the child schema.
    pub(crate) fn input_after(&self) -> Result<RecordBatch> {
        Ok(self.affected_input.clone())
    }

    /// Count keys whose complete input multiset changed, including removed keys.
    pub(crate) fn touched_groups(&self) -> usize {
        self.touched.len()
    }

    /// Count actual rows supplied to the local aggregate evaluation.
    pub(crate) fn aggregate_input_rows(&self) -> usize {
        self.affected_input.num_rows()
    }

    /// Validate local SQL output and produce the next index and exact output delta.
    pub(crate) fn finish(mut self, recomputed_output: &RecordBatch) -> Result<(GroupState, Bag)> {
        if recomputed_output.schema() != self.state.output_schema {
            return Err(Error::Invalid(
                "group aggregate output schema changed".into(),
            ));
        }
        let mut outputs = output_groups(recomputed_output, &self.state.key_schema)?;
        let old_outputs = self
            .touched
            .keys()
            .filter_map(|key| self.state.groups.get(key).map(|bucket| &bucket.output))
            .collect::<Vec<_>>();
        let old = merge_bags(Arc::clone(&self.state.output_schema), &old_outputs)?;
        let new = Bag::from_batch(recomputed_output)?;
        for (key, input) in self.touched {
            if input.is_empty() {
                if outputs.contains_key(&key) {
                    return Err(Error::Invalid(
                        "aggregate output retained a deleted group".into(),
                    ));
                }
                self.state.groups.remove(&key);
            } else {
                let output = outputs.remove(&key).ok_or_else(|| {
                    Error::Invalid("aggregate output omitted a touched group".into())
                })?;
                self.state
                    .groups
                    .insert(key, Arc::new(GroupBucket { input, output }));
            }
        }
        if !outputs.is_empty() {
            return Err(Error::Invalid(
                "aggregate output contains an untouched or unknown group".into(),
            ));
        }
        Ok((self.state, new.difference(&old)?))
    }
}

fn supported_key(data_type: &DataType, policy: KeyPolicy) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64 => true,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => policy.binary_strings,
        _ => false,
    }
}

fn check_pair(input: &RecordBatch, keys: &RecordBatch) -> Result<()> {
    if input.num_rows() != keys.num_rows() {
        return Err(Error::Invalid(
            "group projection must produce exactly one key row per input row".into(),
        ));
    }
    // Reuse Bag's complete-row and byte limits, including variable-width keys.
    Bag::from_batch(input)?;
    Bag::from_batch(keys)?;
    Ok(())
}

fn encode_keys(keys: &RecordBatch) -> Result<Vec<GroupKey>> {
    let converter = RowConverter::new(
        keys.schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(keys.columns())?;
    Ok((0..keys.num_rows())
        .map(|index| rows.row(index).as_ref().to_vec())
        .collect())
}

fn partition(input: &RecordBatch, keys: &RecordBatch) -> Result<BTreeMap<GroupKey, Bag>> {
    let mut indices: BTreeMap<GroupKey, Vec<u32>> = BTreeMap::new();
    for (index, key) in encode_keys(keys)?.into_iter().enumerate() {
        indices
            .entry(key)
            .or_default()
            .push(u32::try_from(index).map_err(|_| Error::Overflow)?);
    }
    indices
        .into_iter()
        .map(|(key, indices)| {
            let count = indices.len();
            let indices = UInt32Array::from(indices);
            let columns = input
                .columns()
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let selected = RecordBatch::try_new_with_options(
                input.schema(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(count)),
            )?;
            Ok((key, Bag::from_batch(&selected)?))
        })
        .collect()
}

fn output_groups(output: &RecordBatch, key_schema: &SchemaRef) -> Result<BTreeMap<GroupKey, Bag>> {
    let key_count = key_schema.fields().len();
    if output.num_columns() < key_count
        || output
            .schema()
            .fields()
            .iter()
            .take(key_count)
            .zip(key_schema.fields())
            .any(|(output, key)| output.data_type() != key.data_type())
    {
        return Err(Error::Invalid(
            "aggregate output must start with matching typed group keys".into(),
        ));
    }
    Bag::from_batch(output)?;
    let keys = output.project(&(0..key_count).collect::<Vec<_>>())?;
    let mut groups = BTreeMap::new();
    for (index, key) in encode_keys(&keys)?.into_iter().enumerate() {
        if groups
            .insert(key, Bag::from_batch(&output.slice(index, 1))?)
            .is_some()
        {
            return Err(Error::Invalid(
                "aggregate output contains a duplicate group key".into(),
            ));
        }
    }
    Ok(groups)
}

fn merge_bags(schema: SchemaRef, bags: &[&Bag]) -> Result<Bag> {
    match bags.len() {
        0 => Ok(Bag::empty(schema)),
        1 => Ok(bags[0].clone()),
        count => {
            let middle = count / 2;
            merge_bags(Arc::clone(&schema), &bags[..middle])?
                .add(&merge_bags(schema, &bags[middle..])?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, NullArray,
        StringArray, StringDictionaryBuilder, StringViewArray, TimestampMillisecondArray,
        UInt16Array, UInt64Array, UInt8Array,
    };
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{i256, Field, Int8Type, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    const BINARY: KeyPolicy = KeyPolicy {
        binary_strings: true,
    };
    type IntRow = (Option<i64>, Option<i64>);

    fn input(rows: &[IntRow]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn keys(input: &RecordBatch) -> RecordBatch {
        input.project(&[0]).unwrap()
    }

    fn output(rows: &[(Option<i64>, i64, Option<i64>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("n", DataType::Int64, false),
                Field::new("sum", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn aggregate(rows: &[IntRow]) -> RecordBatch {
        let mut groups: BTreeMap<Option<i64>, (i64, Option<i64>)> = BTreeMap::new();
        for (key, value) in rows {
            let (count, sum) = groups.entry(*key).or_default();
            *count += 1;
            if let Some(value) = value {
                *sum = Some(sum.unwrap_or(0) + value);
            }
        }
        output(
            &groups
                .into_iter()
                .map(|(key, (count, sum))| (key, count, sum))
                .collect::<Vec<_>>(),
        )
    }

    fn state(rows: &[IntRow]) -> Result<GroupState> {
        let batch = input(rows);
        Ok(GroupState::try_new(&batch, &keys(&batch), &aggregate(rows), BINARY)?.unwrap())
    }

    fn prepare(state: &GroupState, plus: &[IntRow], minus: &[IntRow]) -> Result<GroupUpdate> {
        let plus = input(plus);
        let minus = input(minus);
        state.prepare(&plus, &keys(&plus), &minus, &keys(&minus))
    }

    fn contents(state: &GroupState) -> Result<Bag> {
        merge_bags(
            Arc::clone(&state.output_schema),
            &state
                .groups
                .values()
                .map(|bucket| &bucket.output)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn one_changed_group_materializes_only_its_rows_and_reuses_other_buckets() -> Result<()> {
        let mut rows = (0..100)
            .flat_map(|key| [(Some(key), Some(key)), (Some(key), Some(1))])
            .collect::<Vec<_>>();
        let before = state(&rows)?;
        let old_output = contents(&before)?;
        let candidate = prepare(&before, &[(Some(42), Some(5))], &[(Some(42), Some(1))])?;
        assert_eq!(candidate.touched_groups(), 1);
        assert_eq!(candidate.aggregate_input_rows(), 2);
        assert_eq!(
            Bag::from_batch(&candidate.input_after()?)?,
            Bag::from_batch(&input(&[(Some(42), Some(42)), (Some(42), Some(5))]))?
        );
        let (after, delta) =
            candidate.finish(&aggregate(&[(Some(42), Some(42)), (Some(42), Some(5))]))?;
        rows.retain(|row| row != &(Some(42), Some(1)));
        rows.push((Some(42), Some(5)));
        assert_eq!(after.total_groups(), 100);
        assert_eq!(old_output.add(&delta)?, Bag::from_batch(&aggregate(&rows))?);
        assert_eq!(contents(&after)?, old_output.add(&delta)?);
        assert_eq!(contents(&before)?, old_output);
        assert_eq!(
            before
                .groups
                .iter()
                .filter(|(key, bucket)| Arc::ptr_eq(bucket, &after.groups[*key]))
                .count(),
            99
        );
        Ok(())
    }

    #[test]
    fn cancellation_duplicates_key_move_and_final_delete_are_atomic() -> Result<()> {
        let before = state(&[
            (None, None),
            (Some(1), Some(7)),
            (Some(1), Some(7)),
            (Some(2), Some(8)),
        ])?;
        let cancelled = prepare(
            &before,
            &[(Some(1), Some(7)), (Some(1), Some(7))],
            &[(Some(1), Some(7)), (Some(1), Some(7))],
        )?;
        assert_eq!(cancelled.touched_groups(), 0);
        assert_eq!(cancelled.aggregate_input_rows(), 0);
        assert_eq!(cancelled.input_after()?.schema(), before.input_schema);
        let (same, delta) = cancelled.finish(&output(&[]))?;
        assert!(delta.is_empty());
        assert!(before
            .groups
            .iter()
            .all(|(key, bucket)| Arc::ptr_eq(bucket, &same.groups[key])));
        let changed = prepare(
            &before,
            &[(Some(3), Some(8)), (None, None)],
            &[(Some(2), Some(8)), (Some(1), Some(7))],
        )?;
        assert_eq!(changed.touched_groups(), 4);
        assert_eq!(changed.aggregate_input_rows(), 4);
        let expected = [
            (None, None),
            (None, None),
            (Some(1), Some(7)),
            (Some(3), Some(8)),
        ];
        let (after, delta) = changed.finish(&aggregate(&expected))?;
        assert_eq!(after.total_groups(), 3);
        assert_eq!(contents(&before)?.add(&delta)?, contents(&after)?);
        assert_eq!(contents(&after)?, Bag::from_batch(&aggregate(&expected))?);
        let deleted = prepare(&after, &[], &expected)?;
        assert_eq!(deleted.aggregate_input_rows(), 0);
        assert_eq!(deleted.touched_groups(), 3);
        let (empty, delta) = deleted.finish(&output(&[]))?;
        assert_eq!(empty.total_groups(), 0);
        assert!(contents(&after)?.add(&delta)?.is_empty());
        Ok(())
    }

    #[test]
    fn sum_zero_and_all_null_values_preserve_existing_groups() -> Result<()> {
        let before = state(&[(Some(1), Some(3)), (Some(2), None)])?;
        let update = prepare(&before, &[(Some(1), Some(-3)), (Some(2), None)], &[])?;
        let (after, delta) =
            update.finish(&output(&[(Some(1), 2, Some(0)), (Some(2), 2, None)]))?;
        assert_eq!(after.total_groups(), 2);
        assert_eq!(contents(&before)?.add(&delta)?, contents(&after)?);
        Ok(())
    }

    #[test]
    fn malformed_output_and_absent_deletions_leave_original_state_unchanged() -> Result<()> {
        let before = state(&[(Some(1), Some(3)), (Some(2), Some(8))])?;
        let original = contents(&before)?;
        for bad in [
            output(&[]),
            output(&[(Some(99), 1, Some(3))]),
            output(&[(Some(1), 2, Some(7)), (Some(1), 2, Some(7))]),
            output(&[(Some(1), 2, Some(7)), (Some(2), 1, Some(8))]),
        ] {
            let update = prepare(&before, &[(Some(1), Some(4))], &[])?;
            assert!(update.finish(&bad).is_err());
            assert_eq!(contents(&before)?, original);
        }
        assert!(prepare(&before, &[], &[(Some(99), Some(1))]).is_err());
        assert!(prepare(&before, &[], &[(Some(1), Some(3)), (Some(1), Some(3))]).is_err());
        let removed = prepare(&before, &[], &[(Some(1), Some(3))])?;
        assert!(removed.finish(&output(&[(Some(1), 0, None)])).is_err());
        let update = prepare(&before, &[(Some(1), Some(4))], &[])?;
        let wrong_schema = output(&[(Some(1), 2, Some(7))]).project(&[0, 1])?;
        assert!(update.finish(&wrong_schema).is_err());
        assert_eq!(contents(&before)?, original);
        Ok(())
    }

    #[test]
    fn initialization_and_change_schema_and_row_correspondence_are_checked() -> Result<()> {
        let batch = input(&[(Some(1), Some(3)), (Some(2), Some(8))]);
        for bad in [
            output(&[(Some(1), 1, Some(3))]),
            output(&[(Some(1), 1, Some(3)), (Some(99), 1, Some(8))]),
            output(&[(Some(1), 1, Some(3)), (Some(1), 1, Some(3))]),
        ] {
            assert!(GroupState::try_new(&batch, &keys(&batch), &bad, BINARY).is_err());
        }
        assert!(
            GroupState::try_new(&batch, &keys(&batch).slice(0, 1), &output(&[]), BINARY).is_err()
        );
        let before = state(&[(Some(1), Some(3)), (Some(2), Some(8))])?;
        let plus = input(&[(Some(1), Some(4))]);
        let empty = input(&[]);
        assert!(before
            .prepare(&plus, &keys(&empty), &empty, &keys(&empty))
            .is_err());
        assert!(before
            .prepare(&keys(&plus), &keys(&plus), &empty, &keys(&empty))
            .is_err());
        assert!(before.prepare(&plus, &plus, &empty, &keys(&empty)).is_err());
        let wrong_types = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("1"), Some("2")]))],
        )?;
        assert!(GroupState::try_new(&batch, &keys(&batch), &wrong_types, BINARY).is_err());
        Ok(())
    }

    #[test]
    fn empty_index_and_zero_changes_keep_typed_relations() -> Result<()> {
        let before = state(&[])?;
        let unchanged = prepare(&before, &[], &[])?;
        assert_eq!(unchanged.touched_groups(), 0);
        assert_eq!(unchanged.aggregate_input_rows(), 0);
        let (after, delta) = unchanged.finish(&output(&[]))?;
        assert_eq!(after.total_groups(), 0);
        assert!(delta.is_empty());
        let insertion = prepare(&after, &[(None, None)], &[])?;
        let (after, _) = insertion.finish(&output(&[(None, 1, None)]))?;
        assert_eq!(after.total_groups(), 1);
        Ok(())
    }

    fn key_input(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap()
    }

    async fn oracle(input: &RecordBatch) -> RecordBatch {
        sql_oracle(input, "SELECT k, COUNT(*) AS n FROM t GROUP BY k").await
    }

    async fn sql_oracle(input: &RecordBatch, sql: &str) -> RecordBatch {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_batch("t", input.clone()).unwrap();
        let physical = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let schema = physical.schema();
        concat_batches(&schema, &collect(physical, ctx.task_ctx()).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn admitted_key_partitions_match_independent_sql_equality() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(4)),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
            ])),
            Arc::new(Int8Array::from(vec![Some(-1), Some(1), None, Some(-1)])),
            Arc::new(Int16Array::from(vec![Some(-1), Some(1), None, Some(-1)])),
            Arc::new(Int32Array::from(vec![Some(-1), Some(1), None, Some(-1)])),
            Arc::new(Int64Array::from(vec![Some(-1), Some(1), None, Some(-1)])),
            Arc::new(UInt8Array::from(vec![Some(0), Some(1), None, Some(0)])),
            Arc::new(UInt16Array::from(vec![Some(0), Some(1), None, Some(0)])),
            Arc::new(UInt32Array::from(vec![Some(0), Some(1), None, Some(0)])),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(u64::MAX),
                None,
                Some(0),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(123), Some(-123), None, Some(123)])
                    .with_precision_and_scale(20, 2)?,
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(123)),
                    Some(i256::from_i128(-123)),
                    None,
                    Some(i256::from_i128(123)),
                ])
                .with_precision_and_scale(50, 2)?,
            ),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(20_000),
                None,
                Some(0),
            ])),
            Arc::new(Date64Array::from(vec![
                Some(0),
                Some(86_400_000),
                None,
                Some(0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("a"),
                Some("é"),
                Some("e\u{301}"),
                None,
                Some("A"),
                None,
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("A"),
                Some("a"),
                None,
                Some("A"),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("A"),
                Some("a"),
                None,
                Some("A"),
            ])),
        ];
        for array in arrays {
            let input = key_input(array);
            let expected = oracle(&input).await;
            let state = GroupState::try_new(&input, &input, &expected, BINARY)?.unwrap();
            assert_eq!(
                state.total_groups(),
                expected.num_rows(),
                "{:?}",
                input.schema()
            );
            for bucket in state.groups.values() {
                let batch = bucket.output.to_batch()?;
                let count = match ScalarValue::try_from_array(batch.column(1).as_ref(), 0)? {
                    ScalarValue::Int64(Some(value)) => usize::try_from(value).unwrap(),
                    ScalarValue::UInt64(Some(value)) => usize::try_from(value).unwrap(),
                    other => panic!("unexpected COUNT type {other:?}"),
                };
                assert_eq!(bucket.input.total()?, count, "{:?}", input.schema());
            }
            assert_eq!(contents(&state)?, Bag::from_batch(&expected)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn multiple_epochs_match_full_sql_with_distinct_nulls_and_deletions() -> Result<()> {
        let sql = "SELECT k, COUNT(*) AS n, COUNT(v) AS nv, COUNT(DISTINCT v) AS nd, \
                   SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi \
                   FROM t GROUP BY k";
        let mut rows = vec![
            (None, None),
            (Some(1), Some(2)),
            (Some(1), Some(2)),
            (Some(1), None),
            (Some(2), Some(4)),
            (Some(3), Some(-2)),
            (Some(3), Some(2)),
        ];
        let initial = input(&rows);
        let full = sql_oracle(&initial, sql).await;
        let mut state = GroupState::try_new(&initial, &keys(&initial), &full, BINARY)?.unwrap();
        let epochs: Vec<(Vec<IntRow>, Vec<IntRow>)> = vec![
            (
                vec![(Some(1), Some(4)), (Some(2), None), (Some(4), Some(7))],
                vec![(Some(1), Some(2)), (Some(3), Some(-2)), (Some(3), Some(2))],
            ),
            (vec![(Some(1), Some(2))], vec![(Some(1), Some(2))]),
            (
                vec![(None, None), (Some(5), Some(0))],
                vec![(Some(1), Some(2)), (Some(1), None), (Some(1), Some(4))],
            ),
            (vec![], vec![(Some(2), Some(4)), (Some(2), None)]),
        ];
        for (plus, minus) in epochs {
            let before = contents(&state)?;
            let update = prepare(&state, &plus, &minus)?;
            let recomputed = sql_oracle(&update.input_after()?, sql).await;
            assert_eq!(contents(&state)?, before);
            let (next, delta) = update.finish(&recomputed)?;
            for row in minus {
                let position = rows.iter().position(|candidate| candidate == &row).unwrap();
                rows.remove(position);
            }
            rows.extend(plus);
            let expected = sql_oracle(&input(&rows), sql).await;
            assert_eq!(before.add(&delta)?, Bag::from_batch(&expected)?);
            assert_eq!(contents(&next)?, Bag::from_batch(&expected)?);
            assert_eq!(next.output_schema, state.output_schema);
            state = next;
        }
        Ok(())
    }

    #[test]
    fn changed_group_materialization_enforces_bag_row_and_byte_budgets() -> Result<()> {
        let mut state = state(&[(Some(1), Some(1))])?;
        let key = state.groups.keys().next().unwrap().clone();
        let row = input(&[(Some(1), Some(1))]);
        let output = state.groups[&key].output.clone();
        state.groups.insert(
            key,
            Arc::new(GroupBucket {
                input: Bag::from_weighted(&row, &[crate::bag::MAX_MATERIALIZED_ROWS as i64])?,
                output,
            }),
        );
        assert!(prepare(&state, &[(Some(1), Some(1))], &[]).is_err());

        let schema = Arc::new(Schema::new(
            (0..16)
                .map(|index| Field::new(format!("c{index}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let wide = RecordBatch::try_new(
            Arc::clone(&schema),
            (0..16)
                .map(|_| Arc::new(Int64Array::from(vec![7])) as ArrayRef)
                .collect(),
        )?;
        let mut columns = wide.columns().to_vec();
        columns[15] = Arc::new(Int64Array::from(vec![8]));
        let changed = RecordBatch::try_new(schema, columns)?;
        let mut wide_state = GroupState::try_new(&wide, &keys(&wide), &wide, BINARY)?.unwrap();
        let key = wide_state.groups.keys().next().unwrap().clone();
        let output = wide_state.groups[&key].output.clone();
        wide_state.groups.insert(
            key,
            Arc::new(GroupBucket {
                input: Bag::from_weighted(&wide, &[crate::bag::MAX_MATERIALIZED_ROWS as i64])?,
                output,
            }),
        );
        let result = wide_state.prepare(&changed, &keys(&changed), &wide, &keys(&wide));
        match result {
            Err(Error::Invalid(message)) => assert!(message.contains("-byte safety limit")),
            other => panic!("expected the explicit byte budget error, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn float_dictionary_timestamp_and_unknown_string_equality_use_fallback() -> Result<()> {
        let floats = key_input(Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            None,
        ])));
        let timestamp = key_input(Arc::new(TimestampMillisecondArray::from(vec![
            Some(1),
            None,
        ])));
        let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
        dictionary.append("A")?;
        dictionary.append("A")?;
        dictionary.append_null();
        let dictionary = key_input(Arc::new(dictionary.finish()));
        for unsupported in [floats, timestamp, dictionary] {
            assert!(
                GroupState::try_new(&unsupported, &unsupported, &unsupported, BINARY)?.is_none()
            );
        }
        let strings = key_input(Arc::new(StringArray::from(vec![Some("A"), Some("a")])));
        assert!(GroupState::try_new(
            &strings,
            &strings,
            &strings,
            KeyPolicy {
                binary_strings: false
            }
        )?
        .is_none());
        let empty_keys = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(strings.num_rows())),
        )?;
        assert!(GroupState::try_new(&strings, &empty_keys, &strings, BINARY)?.is_none());
        Ok(())
    }
}
