// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Profile-specific validation against a trusted predecessor relation.

use super::error::CdcError;
use super::generation::Generation;
use super::prepared_batch::PreparedBatch;
use crate::change::{Multiset, Row};
use std::collections::BTreeMap;

/// Delta-sized validation against a trusted predecessor relation.
///
/// The caller asserts that `before` is the complete relation at the batch's predecessor
/// checkpoint. This type checks exactly what [`PreparedBatch::apply_to`] checks without
/// materialising the accumulated relation on every batch:
///
/// * an unkeyed batch needs one multiset lookup per record (`O(delta log n)`);
/// * a keyed batch builds one key index over `before` (`O(n log n)`) and then tracks only the
///   keys the batches touch. A producer-side key index would remove that term; until the
///   producer maintains one, keyed validation stays linear in the relation per call.
///
/// Unkeyed rows the delta does not touch are never inspected, and keyed rows with untouched
/// keys are never compared: this is the same blind spot `apply_to` has, not a weaker check.
pub struct TrustedRelation<'a> {
    generation: &'a Generation,
    state: TrustedState<'a>,
}
enum TrustedState<'a> {
    Unkeyed(UnkeyedTrust<'a>),
    Keyed(KeyedTrust),
}

struct UnkeyedTrust<'a> {
    before: &'a Multiset,
    overlay: Multiset,
}
struct KeyedTrust {
    keys: BTreeMap<Row, Row>,
    /// `None` means the batches deleted the key; `Some` is the row they left behind.
    overlay: BTreeMap<Row, Option<Row>>,
}
impl<'a> TrustedRelation<'a> {
    /// Bind to a trusted relation and check its shape for the generation's profile.
    pub fn new(generation: &'a Generation, before: &'a Multiset) -> Result<Self, CdcError> {
        if generation.schema().key().is_some() {
            let mut keys = BTreeMap::new();
            for (row, weight) in before.iter() {
                if *weight != 1 {
                    return Err(CdcError::InvalidRow);
                }
                let key = generation.schema().key_values(row)?;
                if keys.insert(key, row.clone()).is_some() {
                    return Err(CdcError::InvalidRow);
                }
            }
            Ok(Self {
                generation,
                state: TrustedState::Keyed(KeyedTrust {
                    keys,
                    overlay: BTreeMap::new(),
                }),
            })
        } else {
            Ok(Self {
                generation,
                state: TrustedState::Unkeyed(UnkeyedTrust {
                    before,
                    overlay: Multiset::new(),
                }),
            })
        }
    }
    /// Validate one batch against the effective predecessor relation, then advance it.
    pub fn apply(&mut self, batch: &PreparedBatch) -> Result<(), CdcError> {
        batch.envelope().validate_generation(self.generation)?;
        match &mut self.state {
            TrustedState::Keyed(keyed) => {
                for record in batch.rows() {
                    let key = self.generation.schema().key_values(&record.payload)?;
                    let current = match keyed.overlay.get(&key) {
                        Some(entry) => entry.as_ref(),
                        None => keyed.keys.get(&key),
                    };
                    if record.weight < 0 {
                        if current != Some(&record.payload) {
                            return Err(CdcError::InvalidRow);
                        }
                        keyed.overlay.insert(key, None);
                    } else {
                        if current.is_some() {
                            return Err(CdcError::InvalidRow);
                        }
                        keyed.overlay.insert(key, Some(record.payload.clone()));
                    }
                }
            }
            TrustedState::Unkeyed(unkeyed) => {
                for record in batch.rows() {
                    let prior = unkeyed
                        .before
                        .weight_of(&record.payload)
                        .checked_add(unkeyed.overlay.weight_of(&record.payload))
                        .ok_or(CdcError::Overflow)?;
                    let next = prior.checked_add(record.weight).ok_or(CdcError::Overflow)?;
                    if next < 0 {
                        return Err(CdcError::InvalidRow);
                    }
                    unkeyed
                        .overlay
                        .try_add_row(record.payload.clone(), record.weight)
                        .map_err(|_| CdcError::Overflow)?;
                }
            }
        }
        Ok(())
    }
}
