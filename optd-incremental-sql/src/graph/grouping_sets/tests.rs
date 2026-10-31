use super::*;

#[test]
fn grouping_identity_widths_and_fanout_fail_before_truncation_or_allocation() -> Result<()> {
    for (value, data_type) in [
        (u8::MAX as u64, DataType::UInt8),
        (u16::MAX as u64, DataType::UInt16),
        (u32::MAX as u64, DataType::UInt32),
        (u64::MAX, DataType::UInt64),
    ] {
        let scalar = typed_identity(value, &data_type)?;
        assert_eq!(scalar.data_type(), data_type);
        assert!(
            matches!(
                typed_identity(value.saturating_add(1), &data_type),
                Err(Error::Overflow)
            ) || value == u64::MAX
        );
    }
    assert!(typed_identity(1, &DataType::Int64).is_err());
    assert!(enumerate_sets(&GroupingSet::Cube(vec![lit(1i64); 13])).is_err());
    assert!(enumerate_sets(&GroupingSet::Cube(vec![lit(1i64); usize::BITS as usize])).is_err());
    assert!(enumerate_sets(&GroupingSet::Rollup(vec![lit(1i64); MAX_GROUPING_SETS])).is_err());
    assert!(enumerate_sets(&GroupingSet::GroupingSets(vec![
        Vec::new();
        MAX_GROUPING_SETS + 1
    ]))
    .is_err());
    assert_eq!(
        enumerate_sets(&GroupingSet::Rollup(vec![lit(1i64), lit(2i64)]))?,
        vec![vec![], vec![lit(1i64)], vec![lit(1i64), lit(2i64)]]
    );
    Ok(())
}
