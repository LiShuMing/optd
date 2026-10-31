// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Protocol failures retain their typed source errors.

use std::fmt;

/// A failed protocol validation, ownership check or storage operation.
#[derive(Debug)]
pub enum CdcError {
    /// Schema is ambiguous or unsupported.
    InvalidSchema,
    /// A payload or metadata row violates the contract.
    InvalidRow,
    /// Source identity, sequence or definition does not match.
    InvalidCheckpoint,
    /// Envelope or canonical value is malformed.
    InvalidEnvelope,
    /// Canonical digest does not match the declared value.
    DigestMismatch,
    /// The declared profile is unsupported by this operation.
    UnsupportedProfile,
    /// A bounded protocol integer overflowed.
    Overflow,
    /// Another process holds the required lock.
    Ownership,
    /// A different output successor has appeared.
    Conflict,
    /// The previous attempt's outcome cannot yet be determined.
    UnknownCommit,
    /// A source or view evaluation failed.
    Evaluation(crate::engine::RefreshError),
    /// Local IO failed.
    Io(std::io::Error),
    /// Serialization failed.
    Json(serde_json::Error),
    /// An underlying storage adapter failed.
    Storage(String),
}
impl fmt::Display for CdcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evaluation(e) => write!(f, "CDC evaluation: {e}"),
            Self::Io(e) => write!(f, "CDC IO: {e}"),
            Self::Json(e) => write!(f, "CDC JSON: {e}"),
            Self::Storage(e) => write!(f, "CDC storage: {e}"),
            other => write!(f, "CDC: {other:?}"),
        }
    }
}
impl std::error::Error for CdcError {}
impl From<std::io::Error> for CdcError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for CdcError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
impl From<crate::engine::RefreshError> for CdcError {
    fn from(e: crate::engine::RefreshError) -> Self {
        Self::Evaluation(e)
    }
}
