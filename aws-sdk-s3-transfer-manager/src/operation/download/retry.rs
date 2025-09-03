/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_types::byte_stream::error::Error as ByteStreamError;
use futures_util::future;
use std::sync::Arc;
use tower::retry::budget::{Budget, TpsBudget};

use crate::{
    error::ErrorKind,
    operation::download::{service::DownloadChunkRequest, ChunkOutput},
};

/// A `tower::retry::Policy` implementation for retrying download chunk requests
#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    budget: Arc<TpsBudget>,
    remaining_attempts: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            budget: Arc::new(TpsBudget::default()),
            remaining_attempts: 2,
        }
    }
}

fn find_source<'a, E: std::error::Error + 'static>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a E> {
    let mut next = Some(err);
    while let Some(err) = next {
        if let Some(matching_err) = err.downcast_ref::<E>() {
            return Some(matching_err);
        }
        next = err.source();
    }
    None
}

impl tower::retry::Policy<DownloadChunkRequest, ChunkOutput, crate::error::Error> for RetryPolicy {
    type Future = future::Ready<()>;

    fn retry(
        &mut self,
        req: &mut DownloadChunkRequest,
        result: &mut Result<ChunkOutput, crate::error::Error>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => {
                self.budget.deposit();
                None
            }
            Err(err) => {
                // the only type of error we care about at this point is errors that come from
                // reading the body, all other errors go through the SDK retry implementation
                // already
                find_source::<ByteStreamError>(err)?;
                if self.remaining_attempts == 0 || !self.budget.withdraw() {
                    return None;
                }
                self.remaining_attempts -= 1;
                if let ErrorKind::ChunkFailed(chunk_failed) = err.kind() {
                    req.seq = chunk_failed.download_seq();
                }
                Some(future::ready(()))
            }
        }
    }

    fn clone_request(&mut self, req: &DownloadChunkRequest) -> Option<DownloadChunkRequest> {
        Some(req.clone())
    }
}
