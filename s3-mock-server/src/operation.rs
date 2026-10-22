/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The S3 operations the mock server serves, and the per-operation request counts.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// An S3 operation served by the mock server.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Operation {
    AbortMultipartUpload,
    CompleteMultipartUpload,
    CreateBucket,
    CreateMultipartUpload,
    DeleteBucket,
    DeleteObject,
    GetObject,
    HeadBucket,
    HeadObject,
    ListBuckets,
    ListObjectsV2,
    PutObject,
    UploadPart,
}

impl Operation {
    /// Whether the rate throttle applies to the operation: to every operation except
    /// CreateBucket, DeleteBucket, HeadBucket and ListBuckets.
    pub(crate) fn is_throttled(self) -> bool {
        !matches!(
            self,
            Operation::CreateBucket
                | Operation::DeleteBucket
                | Operation::HeadBucket
                | Operation::ListBuckets
        )
    }
}

/// Number of requests received per operation.
///
/// A request is counted when its handler starts, before the throttle and faults apply, so
/// throttled and failed requests are counted. Requests that s3s rejects before dispatching them
/// to a handler (for example, a signature mismatch) are not.
#[derive(Debug, Default)]
pub(crate) struct RequestCounters {
    counts: Mutex<BTreeMap<Operation, u64>>,
}

impl RequestCounters {
    pub(crate) fn record(&self, operation: Operation) {
        *self.counts.lock().unwrap().entry(operation).or_default() += 1;
    }

    pub(crate) fn get(&self, operation: Operation) -> u64 {
        self.counts
            .lock()
            .unwrap()
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<Operation, u64> {
        self.counts.lock().unwrap().clone()
    }

    pub(crate) fn reset(&self) {
        self.counts.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_each_operation_separately_until_reset() {
        let counters = RequestCounters::default();
        counters.record(Operation::HeadObject);
        counters.record(Operation::HeadObject);
        counters.record(Operation::GetObject);
        assert_eq!(counters.get(Operation::HeadObject), 2);
        assert_eq!(counters.get(Operation::GetObject), 1);
        assert_eq!(counters.get(Operation::ListObjectsV2), 0);
        assert_eq!(
            counters.snapshot(),
            BTreeMap::from([(Operation::GetObject, 1), (Operation::HeadObject, 2)])
        );
        counters.reset();
        assert_eq!(counters.get(Operation::HeadObject), 0);
        assert!(counters.snapshot().is_empty());
    }
}
