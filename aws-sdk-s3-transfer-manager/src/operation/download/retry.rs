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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ChunkId, Error, ErrorKind};
    use crate::operation::download::{DownloadContext, DownloadInputBuilder};
    use crate::types::{BucketType, PartSize};
    use std::io;
    use std::sync::Arc;
    use tower::retry::Policy;

    fn test_handle(
        client: aws_sdk_s3::Client,
        target_part_size: u64,
    ) -> Arc<crate::client::Handle> {
        let tm_config = crate::Config::builder()
            .client(client)
            .set_target_part_size(PartSize::Target(target_part_size))
            .build();
        let tm = crate::Client::new(tm_config);
        tm.handle.clone()
    }

    fn download_chunk_request_for_tests() -> DownloadChunkRequest {
        let client = aws_sdk_s3::Client::from_conf(aws_sdk_s3::Config::builder().build());
        let target_part_size = 50;
        let handle = test_handle(client, target_part_size);
        let ctx = DownloadContext::new(handle, BucketType::Standard);
        DownloadChunkRequest {
            ctx,
            remaining: 0..=1023,
            input: DownloadInputBuilder::default(),
            start_seq: 0,
            seq: None,
        }
    }

    #[test]
    fn test_retry_updates_seq_on_chunk_failed_error() {
        let mut policy = RetryPolicy::default();
        let mut req = download_chunk_request_for_tests();
        let io_err = io::Error::new(io::ErrorKind::Other, "test error");
        let bytestream_err = ByteStreamError::from(io_err);
        let mut result = Err(crate::error::chunk_failed(
            ChunkId::Download(5),
            bytestream_err,
        ));

        policy.retry(&mut req, &mut result);
        assert_eq!(Some(5), req.seq);
    }

    #[test]
    fn test_retry_does_not_update_seq_on_non_chunk_failed_error() {
        let mut policy = RetryPolicy::default();
        let mut req = download_chunk_request_for_tests();
        let mut result = Err(Error::new(ErrorKind::RuntimeError, "test error"));

        policy.retry(&mut req, &mut result);
        assert_eq!(None, req.seq);
    }
}
