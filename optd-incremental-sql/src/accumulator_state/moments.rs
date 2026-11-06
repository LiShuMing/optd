//! Centered, retractable moments with counted-value numerical rebasing.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;

use super::value_index::OrderedValues;
use crate::metrics::{NumericWork, ValueIndexWork};
use crate::{Error, Result};

/// Output choice shares one numerical algorithm and explicit empty conventions.
#[derive(Clone, Copy, Debug)]
pub(super) enum MomentOutput {
    Mean,
    SampleStddev,
    PopulationStddev,
}

/// Translated coordinates preserve small deviations around a large finite origin.
#[derive(Clone, Debug, Default)]
pub(super) struct Moments {
    count: u64,
    origin: f64,
    mean: f64,
    m2: f64,
    values: OrderedValues,
}

impl Moments {
    fn value(scalar: &ScalarValue) -> Result<Option<f64>> {
        match scalar {
            ScalarValue::Float64(None) => Ok(None),
            ScalarValue::Float64(Some(value)) if value.is_finite() => Ok(Some(*value)),
            _ => Err(Error::NumericDomain),
        }
    }

    /// Weighted parallel-Welford merge, also used to rebase without expanding duplicates.
    fn add(&mut self, value: f64, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            self.origin = value;
            self.mean = 0.0;
            self.m2 = 0.0;
            self.count = count;
            return Ok(());
        }
        let total = self.count.checked_add(count).ok_or(Error::Overflow)?;
        let delta = (value - self.origin) - self.mean;
        self.mean += delta * (count as f64 / total as f64);
        self.m2 += delta * (delta * (self.count as f64 / total as f64)) * count as f64;
        self.count = total;
        Ok(())
    }

    fn remove(&mut self, value: f64) -> Result<()> {
        self.count = self.count.checked_sub(1).ok_or(Error::NumericDomain)?;
        if self.count == 0 {
            self.origin = 0.0;
            self.mean = 0.0;
            self.m2 = 0.0;
            return Ok(());
        }
        let delta = (value - self.origin) - self.mean;
        self.mean -= delta / self.count as f64;
        self.m2 -= delta * ((value - self.origin) - self.mean);
        Ok(())
    }

    /// Re-evaluate this touched group in canonical value order for exact recovery.
    fn rebase(&mut self, work: &mut NumericWork) -> Result<()> {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        let values = self.values.clone();
        for (value, count) in values.entries() {
            work.rebase_values = work.rebase_values.checked_add(1).ok_or(Error::Overflow)?;
            self.add(Self::value(value)?.ok_or(Error::NumericDomain)?, count)?;
        }
        work.rebases = work.rebases.checked_add(1).ok_or(Error::Overflow)?;
        Ok(())
    }

    /// Update private counted state first; invalid inputs never affect installed state.
    pub(super) fn apply(
        &mut self,
        positive: &ArrayRef,
        negative: &ArrayRef,
        kind: MomentOutput,
        work: &mut NumericWork,
    ) -> Result<ScalarValue> {
        for array in [negative, positive] {
            for i in 0..array.len() {
                Self::value(&ScalarValue::try_from_array(array, i)?)?;
            }
        }
        let converter = RowConverter::new(vec![SortField::new(DataType::Float64)])?;
        let mut index_work = ValueIndexWork::default();
        self.values
            .apply(&converter, positive, negative, &mut index_work)?;
        work.directory_entries_copied = work
            .directory_entries_copied
            .checked_add(index_work.directory_entries_copied)
            .ok_or(Error::Overflow)?;
        for (array, remove) in [(negative, true), (positive, false)] {
            for i in 0..array.len() {
                if let Some(value) = Self::value(&ScalarValue::try_from_array(array, i)?)? {
                    if remove {
                        self.remove(value)?;
                    } else {
                        self.add(value, 1)?;
                    }
                    work.observations = work.observations.checked_add(1).ok_or(Error::Overflow)?;
                }
            }
        }
        // Checkpoints currently rebuild operator state from sources. Canonical value-order
        // rebasing makes the published endpoint independent of refresh history and restart.
        self.rebase(work)?;
        if self.m2 < 0.0 || !self.m2.is_finite() || !self.mean.is_finite() {
            return Err(Error::NumericDomain);
        }
        let result = match kind {
            MomentOutput::Mean if self.count > 0 => Some(self.origin + self.mean),
            MomentOutput::SampleStddev if self.count > 1 => {
                Some((self.m2 / (self.count - 1) as f64).sqrt())
            }
            MomentOutput::PopulationStddev if self.count > 0 => {
                Some((self.m2 / self.count as f64).sqrt())
            }
            _ => None,
        };
        if result.is_some_and(|value| !value.is_finite()) {
            return Err(Error::NumericDomain);
        }
        Ok(ScalarValue::Float64(result))
    }
}
