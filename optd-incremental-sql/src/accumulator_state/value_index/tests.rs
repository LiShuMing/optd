use super::*;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::SortField;

fn array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn converter() -> RowConverter {
    RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap()
}
fn endpoints(values: &OrderedValues) -> (Option<ScalarValue>, Option<ScalarValue>) {
    let mut work = ValueIndexWork::default();
    (
        values.value(Extremum::Minimum, &mut work).unwrap().cloned(),
        values.value(Extremum::Maximum, &mut work).unwrap().cloned(),
    )
}

#[test]
fn arbitrary_deletes_and_discarded_candidates_match_an_independent_ordered_map() -> Result<()> {
    let converter = converter();
    let mut installed = OrderedValues::default();
    let mut model = BTreeMap::<i64, usize>::new();
    let mut seed = 7919u64;
    for step in 0..240 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let value = (seed % 71) as i64 - 35;
        let remove = model.get(&value).is_some_and(|n| *n > 0) && step % 3 != 0;
        let mut candidate = installed.clone();
        let mut work = ValueIndexWork::default();
        let (positive, negative) = if remove {
            (array(vec![None]), array(vec![Some(value)]))
        } else {
            (array(vec![Some(value)]), array(vec![None]))
        };
        candidate.apply(&converter, &positive, &negative, &mut work)?;
        assert_eq!(work.input_rows, 2);
        assert_eq!(work.value_updates, 1);
        assert!(
            work.directory_entries_copied
                <= 4 * (installed.values.len().max(1).ilog2() as usize + 2)
        );
        let old = endpoints(&installed);
        if step % 11 == 0 {
            drop(candidate);
            assert_eq!(endpoints(&installed), old);
            continue;
        }
        if remove {
            let count = model.get_mut(&value).unwrap();
            *count -= 1;
            if *count == 0 {
                model.remove(&value);
            }
        } else {
            *model.entry(value).or_default() += 1;
        }
        installed = candidate;
        assert_eq!(installed.values.len(), model.len());
        assert_eq!(
            endpoints(&installed),
            (
                model
                    .first_key_value()
                    .map(|(v, _)| ScalarValue::Int64(Some(*v))),
                model
                    .last_key_value()
                    .map(|(v, _)| ScalarValue::Int64(Some(*v)))
            )
        );
        for (encoded, entry) in installed.values.iter() {
            let ScalarValue::Int64(Some(value)) = entry.value else {
                panic!("integer oracle");
            };
            assert_eq!(entry.count, model[&value] as i64);
            assert_eq!(
                converter
                    .convert_columns(&[array(vec![Some(value)])])?
                    .row(0)
                    .as_ref(),
                encoded
            );
        }
    }
    Ok(())
}

#[test]
fn overflow_negative_counts_and_corrupt_identity_fail_without_mutating_installed_state(
) -> Result<()> {
    let converter = converter();
    let key = converter
        .convert_columns(&[array(vec![Some(7)])])?
        .row(0)
        .as_ref()
        .to_vec();
    let mut installed = OrderedValues::default();
    installed.values.insert(
        key.clone(),
        Arc::new(ValueCount {
            value: ScalarValue::Int64(Some(7)),
            count: i64::MAX,
        }),
    );
    let old = endpoints(&installed);
    let mut candidate = installed.clone();
    assert!(matches!(
        candidate.apply(
            &converter,
            &array(vec![Some(7)]),
            &array(vec![]),
            &mut ValueIndexWork::default()
        ),
        Err(Error::Overflow)
    ));
    assert_eq!(endpoints(&installed), old);
    assert_eq!(installed.values.get(&key).unwrap().count, i64::MAX);
    let mut candidate = installed.clone();
    assert!(candidate
        .apply(
            &converter,
            &array(vec![Some(-100)]),
            &array(vec![Some(8)]),
            &mut ValueIndexWork::default()
        )
        .is_err());
    assert_eq!(endpoints(&installed), old);
    assert_eq!(installed.values.len(), 1);
    let mut corrupt = installed.clone();
    corrupt.values.insert(
        key,
        Arc::new(ValueCount {
            value: ScalarValue::Int64(Some(8)),
            count: 1,
        }),
    );
    assert!(corrupt
        .apply(
            &converter,
            &array(vec![Some(7)]),
            &array(vec![]),
            &mut ValueIndexWork::default()
        )
        .is_err());
    Ok(())
}
