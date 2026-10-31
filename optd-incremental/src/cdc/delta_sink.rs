// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! CDC publication contract, separate from concrete storage adapters.

use super::commit_envelope::CommitEnvelope;
use super::error::CdcError;
use super::prepared_batch::PreparedBatch;
use std::future::Future;

/// Output checkpoint authority; implementations return committed metadata, never just `()`.
pub trait DeltaSink {
    /// Inspect the durable producer checkpoint without advancing it.
    fn last_checkpoint(
        &self,
    ) -> impl Future<Output = Result<Option<CommitEnvelope>, CdcError>> + Send;
    /// Publish the exact prepared batch, resolving duplicates before another append.
    fn emit(
        &mut self,
        batch: &PreparedBatch,
    ) -> impl Future<Output = Result<CommitEnvelope, CdcError>> + Send;
}
