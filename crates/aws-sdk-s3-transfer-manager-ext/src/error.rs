/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_execution::RuntimeId;
use aws_smithy_managed_http::client::PartitionError;
use std::{error::Error, fmt};

/// Failure to create an HTTP view for a managed runtime and pool.
#[non_exhaustive]
#[derive(Debug)]
pub struct HttpClientError {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    RuntimeMismatch {
        execution: RuntimeId,
        pool: RuntimeId,
    },
    Partition(PartitionError),
}

impl HttpClientError {
    pub(crate) fn runtime_mismatch(execution: RuntimeId, pool: RuntimeId) -> Self {
        Self {
            kind: Kind::RuntimeMismatch { execution, pool },
        }
    }

    pub(crate) fn partition(source: PartitionError) -> Self {
        Self {
            kind: Kind::Partition(source),
        }
    }
}

impl fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            Kind::RuntimeMismatch { execution, pool } => write!(
                f,
                "HTTP pool belongs to {pool}, but execution belongs to {execution}"
            ),
            Kind::Partition(source) => write!(f, "unable to resolve HTTP partition: {source}"),
        }
    }
}

impl Error for HttpClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            Kind::RuntimeMismatch { .. } => None,
            Kind::Partition(source) => Some(source),
        }
    }
}
