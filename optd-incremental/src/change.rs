// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Change representation for incremental view maintenance.
//!
//! A **relation** and a **delta** are both oriented multisets: a map from row to
//! an integer weight. The only difference is the invariant that a relation's
//! weights are non-negative. Using one representation for both is what makes the
//! algebra in `rules.rs` short: union is `+`, retraction is `-`, and
//! "the state before the batch" is the current state minus the batch.
//!
//! Weights, not insert-only records: a change to a row that participates in a
//! join or group key is a retraction *and* an insertion, and a delete has no
//! record that can report itself. See `docs/src/incremental/change_capture.md`.

use std::collections::BTreeMap;
use std::fmt;

/// A monotonic feed position. `changes(from, to)` is the half-open range `(from, to]`.
pub type Position = u64;

/// Table identifier, local to a source.
pub type TableId = u32;

/// A single column value. Deliberately small: the framework is about the
/// *algebra*, not about a type system.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
pub enum Value {
    /// SQL NULL, including its unknown comparison semantics in the evaluator.
    Null,
    /// Signed 64-bit integer.
    Int(i64),
    /// Owned UTF-8 string with binary equality.
    Str(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(i) => write!(f, "{}", i),
            Value::Str(s) => write!(f, "{}", s),
        }
    }
}

/// A row is a vector of values.
pub type Row = Vec<Value>;

/// An oriented multiset of rows. A relation has all weights `>= 0`; a delta may
/// have negative weights (retractions). A weight of `0` means "absent" and is
/// never stored.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Multiset {
    entries: BTreeMap<Row, i64>,
}

/// Arithmetic that would corrupt signed multiplicities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArithmeticError;

impl fmt::Display for ArithmeticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "signed multiset arithmetic overflow")
    }
}
impl std::error::Error for ArithmeticError {}

impl Multiset {
    /// Add a weight without wrapping or mutating on overflow.
    pub fn try_add_row(&mut self, row: Row, weight: i64) -> Result<(), ArithmeticError> {
        let next = self
            .entries
            .get(&row)
            .copied()
            .unwrap_or(0)
            .checked_add(weight)
            .filter(|n| *n != i64::MIN)
            .ok_or(ArithmeticError)?;
        if next == 0 {
            self.entries.remove(&row);
        } else {
            self.entries.insert(row, next);
        }
        Ok(())
    }

    /// Checked union, leaving both inputs unchanged on failure.
    pub fn try_union(&self, other: &Multiset) -> Result<Self, ArithmeticError> {
        let mut out = self.clone();
        for (row, weight) in other.iter() {
            out.try_add_row(row.clone(), *weight)?;
        }
        Ok(out)
    }

    /// Checked weight negation.
    pub fn try_negate(&self) -> Result<Self, ArithmeticError> {
        let mut out = Self::new();
        for (row, weight) in self.iter() {
            out.try_add_row(row.clone(), weight.checked_neg().ok_or(ArithmeticError)?)?;
        }
        Ok(out)
    }

    /// Checked endpoint difference.
    pub fn try_difference(&self, other: &Multiset) -> Result<Self, ArithmeticError> {
        self.try_union(&other.try_negate()?)
    }

    /// Construct an empty relation or signed change set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a positive relation, counting repeated full rows as duplicates.
    /// Multiplicities must fit in `i64`; this convenience constructor is unchecked.
    pub fn from_rows(rows: impl IntoIterator<Item = Row>) -> Self {
        let mut ms = Self::new();
        for r in rows {
            ms.add_row(r, 1);
        }
        ms
    }

    /// Add `weight` copies of `row`. A resulting weight of zero removes the row.
    /// This convenience method uses unchecked arithmetic; use [`Self::try_add_row`]
    /// when weights come from untrusted input or can approach integer limits.
    pub fn add_row(&mut self, row: Row, weight: i64) {
        if weight == 0 {
            return;
        }
        match self.entries.get_mut(&row) {
            Some(w) => {
                *w += weight;
                if *w == 0 {
                    self.entries.remove(&row);
                }
            }
            None => {
                self.entries.insert(row, weight);
            }
        }
    }

    /// Add the other's weights in place using unchecked arithmetic.
    /// Use [`Self::try_union`] when overflow must be reported without partial mutation.
    pub fn merge(&mut self, other: &Multiset) {
        for (row, w) in other.iter() {
            self.add_row(row.clone(), *w);
        }
    }

    /// Multiset union: weights add. `A ∪ B` (this is what `Δ` rules combine with).
    /// Uses unchecked arithmetic; [`Self::try_union`] reports overflow.
    pub fn union(&self, other: &Multiset) -> Multiset {
        let mut out = self.clone();
        out.merge(other);
        out
    }

    /// Multiset difference `self ⊖ other`: negate the other's weights and add.
    /// This is how `Σ(before) = Σ(after) ⊖ Δ` is realised.
    /// Uses unchecked arithmetic; [`Self::try_difference`] reports overflow.
    pub fn difference(&self, other: &Multiset) -> Multiset {
        self.union(&other.negate())
    }

    /// `⊖ self`: flip every weight.
    /// Uses unchecked arithmetic; [`Self::try_negate`] reports overflow.
    pub fn negate(&self) -> Multiset {
        let mut out = Multiset::new();
        for (row, w) in self.iter() {
            out.add_row(row.clone(), -w);
        }
        out
    }

    /// Visit distinct stored rows and nonzero weights in deterministic row order.
    pub fn iter(&self) -> impl Iterator<Item = (&Row, &i64)> {
        self.entries.iter()
    }

    /// Weight of one exact row; zero when absent.
    pub fn weight_of(&self, row: &Row) -> i64 {
        self.entries.get(row).copied().unwrap_or(0)
    }

    /// Whether consolidation left no rows with nonzero weight.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Count distinct stored rows, independent of their multiplicities or signs.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Total number of rows counting multiplicity (negative weights contribute
    /// negatively, so this is only meaningful for relations).
    /// Uses unchecked summation; the total must fit in `i64`.
    pub fn total(&self) -> i64 {
        self.entries.values().sum()
    }

    /// A relation must not contain retractions.
    pub fn is_valid_relation(&self) -> bool {
        self.entries.values().all(|w| *w >= 0)
    }
}

/// What a change feed can express. These flags — not the operator set — decide
/// whether a view may be maintained incrementally
/// (`docs/src/incremental/change_capture.md` §7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Capabilities {
    /// The feed is totally ordered by its position.
    pub ordered: bool,
    /// No missing ranges are possible.
    pub gap_free: bool,
    /// Deletions are representable. A feed that only reports inserts cannot
    /// maintain a view over a mutable table, and fails *silently*.
    pub expresses_deletes: bool,
    /// Updates carry the row's previous image, needed whenever a changed column
    /// participates in matching or grouping.
    pub before_images: bool,
    /// Positions are comparable across tables, so a multi-table view is not torn.
    pub cross_table_position: bool,
}

impl Capabilities {
    /// A full change log: everything the framework needs.
    pub fn change_log() -> Self {
        Self {
            ordered: true,
            gap_free: true,
            expresses_deletes: true,
            before_images: true,
            cross_table_position: true,
        }
    }

    /// A snapshot-diff feed: no source cooperation, but it reads whole tables.
    pub fn snapshot_diff() -> Self {
        Self {
            ordered: true,
            gap_free: true,
            expresses_deletes: true,
            before_images: true,
            cross_table_position: false,
        }
    }

    /// A timestamp/version-column feed. The trap: it cannot see deletes.
    pub fn version_column() -> Self {
        Self {
            ordered: true,
            gap_free: true,
            expresses_deletes: false,
            before_images: false,
            cross_table_position: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(i: i64) -> Row {
        vec![Value::Int(i)]
    }

    #[test]
    fn cancelling_weights_remove_the_row() {
        let mut ms = Multiset::new();
        ms.add_row(row(1), 1);
        assert_eq!(ms.len(), 1);
        ms.add_row(row(1), -1);
        assert!(ms.is_empty(), "a +1 then -1 must cancel");
    }

    #[test]
    fn union_adds_weights() {
        let a = Multiset::from_rows([row(1), row(2)]);
        let b = Multiset::from_rows([row(2), row(3)]);
        let u = a.union(&b);
        // row(2) is present twice: {1:1, 2:2, 3:1}
        let counts: Vec<i64> = u.iter().map(|(_, w)| *w).collect();
        assert_eq!(counts, vec![1, 2, 1]);
    }

    #[test]
    fn difference_recovers_the_before_state() {
        // after ⊖ delta == before, which is the Σ(before) simplification.
        let before = Multiset::from_rows([row(1), row(2)]);
        let delta = Multiset::from_rows([row(3)]).union(&Multiset::from_rows([row(2)]).negate());
        let after = before.union(&delta);
        assert_eq!(after.difference(&delta), before);
    }

    #[test]
    fn relations_reject_retractions() {
        let mut ms = Multiset::from_rows([row(1)]);
        assert!(ms.is_valid_relation());
        ms.add_row(row(1), -1);
        assert!(ms.is_valid_relation(), "cancelled, so still a relation");
        ms.add_row(row(9), -1);
        assert!(!ms.is_valid_relation());
    }
}
