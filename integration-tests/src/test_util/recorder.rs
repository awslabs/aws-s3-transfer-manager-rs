/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`OperationRecorder`]: an S3 client interceptor that records each request attempt.

use std::sync::{Arc, Mutex};

use aws_s3_transfer::types::ChecksumAlgorithm;
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_smithy_runtime_api::client::orchestrator::Metadata;

/// One request attempt the S3 client sent.
#[derive(Debug, Clone)]
pub(crate) struct RecordedAttempt {
    /// The operation name.
    pub(crate) operation: String,
    /// The checksum algorithm the request carried a value for, as an `x-amz-checksum-<algorithm>`
    /// header or as an aws-chunked trailer named by `x-amz-trailer`. `None` when it carried no
    /// checksum.
    pub(crate) checksum: Option<ChecksumAlgorithm>,
}

/// The checksum algorithm whose value `headers` carry or name as a trailer.
///
/// `x-amz-checksum-type` and `x-amz-checksum-mode` name no algorithm and are skipped. A header
/// for an algorithm the SDK does not model maps to `ChecksumAlgorithm::Unknown`.
fn sent_checksum(headers: &aws_smithy_runtime_api::http::Headers) -> Option<ChecksumAlgorithm> {
    const PREFIX: &str = "x-amz-checksum-";
    let header_names = headers.iter().map(|(name, _)| name);
    let trailer_names = headers.get("x-amz-trailer").into_iter();
    header_names
        .chain(trailer_names)
        .map(str::to_ascii_lowercase)
        .find_map(|name| {
            let algorithm = name.strip_prefix(PREFIX)?;
            (algorithm != "type" && algorithm != "mode")
                .then(|| ChecksumAlgorithm::from(algorithm.to_ascii_uppercase().as_str()))
        })
}

/// Records every request attempt the S3 client sends.
#[derive(Debug, Clone, Default)]
pub(crate) struct OperationRecorder {
    /// Attempts in send order, one entry per attempt.
    seen: Arc<Mutex<Vec<RecordedAttempt>>>,
}

impl OperationRecorder {
    /// Recorded attempts of `operation`, in send order.
    pub(crate) fn attempts(&self, operation: &str) -> Vec<RecordedAttempt> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.operation == operation)
            .cloned()
            .collect()
    }

    /// Number of recorded attempts of `operation`.
    pub(crate) fn count(&self, operation: &str) -> usize {
        self.attempts(operation).len()
    }
}

impl Intercept for OperationRecorder {
    fn name(&self) -> &'static str {
        "OperationRecorder"
    }

    // Runs once per attempt, so SDK retries are counted.
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(metadata) = cfg.load::<Metadata>() {
            self.seen.lock().unwrap().push(RecordedAttempt {
                operation: metadata.name().to_string(),
                checksum: sent_checksum(context.request().headers()),
            });
        }
        Ok(())
    }
}
