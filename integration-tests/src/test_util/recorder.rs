/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`OperationRecorder`]: an S3 client interceptor that records each request attempt.

use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_smithy_runtime_api::client::orchestrator::Metadata;

/// Records the operation name of every request attempt the S3 client sends.
#[derive(Debug, Clone, Default)]
pub(crate) struct OperationRecorder {
    /// Operation names in send order, one entry per attempt.
    seen: Arc<Mutex<Vec<String>>>,
}

impl OperationRecorder {
    /// Number of recorded attempts of `operation`.
    pub(crate) fn count(&self, operation: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|op| *op == operation)
            .count()
    }
}

impl Intercept for OperationRecorder {
    fn name(&self) -> &'static str {
        "OperationRecorder"
    }

    // Runs once per attempt, so SDK retries are counted.
    fn read_before_transmit(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(metadata) = cfg.load::<Metadata>() {
            self.seen.lock().unwrap().push(metadata.name().to_string());
        }
        Ok(())
    }
}
