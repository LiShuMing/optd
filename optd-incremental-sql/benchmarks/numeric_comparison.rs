//! Numerical agreement is separate from exact source bags and signed endpoint replay.

use datafusion::arrow::array::RecordBatch;
use datafusion::common::ScalarValue;
use std::collections::{HashMap, VecDeque};

use super::{equal, Result};

pub(super) const ABSOLUTE: f64 = 1e-10;
pub(super) const RELATIVE: f64 = 1e-9;

fn compatible(left: &[ScalarValue], right: &[ScalarValue]) -> bool {
    left.iter().zip(right).all(|(a, b)| {
        if a == b {
            return true;
        }
        let pair = match (a, b) {
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => Some((*a, *b)),
            (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
                Some((*a as f64, *b as f64))
            }
            _ => None,
        };
        pair.is_some_and(|(a, b)| {
            a.is_finite() && b.is_finite() && (a - b).abs() <= ABSOLUTE + RELATIVE * b.abs()
        })
    })
}

/// Match whole rows one-to-one: exact unmarked keys, bounded marked values, exact multiplicity.
pub(super) fn equal_result(
    left: &RecordBatch,
    right: &RecordBatch,
    columns: &[usize],
) -> Result<()> {
    if columns.is_empty() {
        return equal(left, right);
    }
    if left
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name(), f.data_type()))
        .ne(right
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name(), f.data_type())))
        || left.num_rows() != right.num_rows()
    {
        return Err("numerical result schema/cardinality mismatch".into());
    }
    type Groups = HashMap<Vec<ScalarValue>, Vec<Vec<ScalarValue>>>;
    let group = |batch: &RecordBatch| -> Result<Groups> {
        let mut groups = Groups::new();
        for i in 0..batch.num_rows() {
            let mut exact = Vec::new();
            let mut numerical = Vec::new();
            for (column, array) in batch.columns().iter().enumerate() {
                let value = ScalarValue::try_from_array(array, i)?;
                if columns.contains(&column) {
                    numerical.push(value);
                } else {
                    exact.push(value);
                }
            }
            groups.entry(exact).or_default().push(numerical);
        }
        Ok(groups)
    };
    let a = group(left)?;
    let mut b = group(right)?;
    for (key, left) in a {
        let right = b
            .remove(&key)
            .ok_or("exact key/membership changed under numerical comparison")?;
        if left.len() != right.len() {
            return Err("numerical result multiplicity mismatch".into());
        }
        let mut left_match: Vec<Option<usize>> = vec![None; left.len()];
        let mut right_match: Vec<Option<usize>> = vec![None; right.len()];
        for start in 0..left.len() {
            let mut queue = VecDeque::from([start]);
            let mut seen_left = vec![false; left.len()];
            seen_left[start] = true;
            let mut parents = vec![None; right.len()];
            let mut free = None;
            'search: while let Some(l) = queue.pop_front() {
                for r in 0..right.len() {
                    if parents[r].is_some() || !compatible(&left[l], &right[r]) {
                        continue;
                    }
                    parents[r] = Some(l);
                    match right_match[r] {
                        None => {
                            free = Some(r);
                            break 'search;
                        }
                        Some(next) if !seen_left[next] => {
                            seen_left[next] = true;
                            queue.push_back(next);
                        }
                        _ => {}
                    }
                }
            }
            let mut r = free.ok_or("numerical result exceeds absolute/relative error contract")?;
            loop {
                let l = parents[r].ok_or("invalid numerical matching path")?;
                let previous = left_match[l];
                left_match[l] = Some(r);
                right_match[r] = Some(l);
                match previous {
                    None => break,
                    Some(previous) => r = previous,
                }
            }
        }
    }
    if !b.is_empty() {
        return Err("numerical result contains unmatched exact keys".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    fn batch(keys: Vec<i64>, values: Vec<Option<f64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }
    #[test]
    fn tolerance_never_changes_keys_nulls_counts_or_unmarked_values() {
        let a = batch(vec![1, 1], vec![Some(2.0), None]);
        let b = batch(vec![1, 1], vec![None, Some(2.0 + 1e-10)]);
        assert!(equal_result(&a, &b, &[1]).is_ok());
        assert!(equal_result(&a, &b, &[]).is_err());
        assert!(equal_result(&a, &batch(vec![1, 2], vec![None, Some(2.0)]), &[1]).is_err());
        assert!(equal_result(&a, &batch(vec![1, 1], vec![Some(2.0), Some(2.0)]), &[1]).is_err());
        assert!(equal_result(&a, &batch(vec![1, 1], vec![None, Some(2.1)]), &[1]).is_err());
    }
}
