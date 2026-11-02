/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    cell::Cell,
    fmt,
    marker::PhantomData,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

/// An opaque runtime identity, unique within this process.
///
/// Identities are not reused when a runtime is dropped.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeId(pub(crate) usize);

impl RuntimeId {
    pub(crate) fn allocate() -> Option<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(1);
        Self::allocate_from(&NEXT)
    }

    fn allocate_from(next: &AtomicUsize) -> Option<Self> {
        let mut id = next.load(Ordering::Relaxed);
        loop {
            let following = id.checked_add(1)?;
            match next.compare_exchange_weak(id, following, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Some(Self(id)),
                Err(observed) => id = observed,
            }
        }
    }
}

impl fmt::Display for RuntimeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "runtime-{}", self.0)
    }
}

/// Identifies one execution partition within a particular runtime.
///
/// Equal indices in different runtimes have different identities. This is not
/// a processor identifier or an HTTP pool partition identifier.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimePartitionId {
    runtime: RuntimeId,
    index: usize,
}

impl RuntimePartitionId {
    pub(crate) fn new(runtime: RuntimeId, index: usize) -> Self {
        Self { runtime, index }
    }

    /// Returns the runtime that owns this partition.
    pub fn runtime_id(self) -> RuntimeId {
        self.runtime
    }

    /// Returns this partition's zero-based index within its runtime.
    ///
    /// The index is the partition's position in its runtime's partition slice,
    /// which follows the supplied topology's placement ordering. Processor
    /// identifiers do not determine these indices.
    pub fn index(self) -> usize {
        self.index
    }
}

impl fmt::Display for RuntimePartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/partition-{}", self.runtime, self.index)
    }
}

thread_local! {
    static CURRENT: Cell<Option<RuntimePartitionId>> = const { Cell::new(None) };
    static RUNTIME_THREAD: Cell<Option<RuntimeId>> = const { Cell::new(None) };
}

// This records OS-thread ownership, including Tokio blocking-pool threads. It
// lasts for that thread's lifetime and is independent of entered Tokio contexts.
pub(crate) fn mark_runtime_thread(runtime: RuntimeId) {
    RUNTIME_THREAD.with(|current| current.set(Some(runtime)));
}

pub(crate) fn owns_current_thread(runtime: RuntimeId) -> bool {
    RUNTIME_THREAD.with(|current| current.get() == Some(runtime))
}

pub(crate) fn current_partition() -> Option<RuntimePartitionId> {
    CURRENT.with(Cell::get)
}

pub(crate) struct CurrentPartition {
    previous: Option<RuntimePartitionId>,
    _thread: PhantomData<Rc<()>>,
}

impl CurrentPartition {
    pub(crate) fn enter(id: RuntimePartitionId) -> Self {
        Self {
            previous: CURRENT.with(|current| current.replace(Some(id))),
            _thread: PhantomData,
        }
    }
}

impl Drop for CurrentPartition {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(self.previous));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, thread};

    // ref: awslabs/aws-s3-transfer-manager-rs#217
    #[test]
    fn identity_exhaustion_does_not_wrap_or_reuse_ids() {
        let next = AtomicUsize::new(usize::MAX - 1);
        assert_eq!(
            RuntimeId::allocate_from(&next),
            Some(RuntimeId(usize::MAX - 1))
        );
        assert_eq!(RuntimeId::allocate_from(&next), None);
        assert_eq!(RuntimeId::allocate_from(&next), None);
        assert_eq!(next.load(Ordering::Relaxed), usize::MAX);
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#217
    #[test]
    fn concurrent_identity_allocation_never_reuses_an_id() {
        let next = AtomicUsize::new(1);
        let ids = thread::scope(|scope| {
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..128)
                            .map(|_| RuntimeId::allocate_from(&next).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            tasks
                .into_iter()
                .flat_map(|task| task.join().unwrap())
                .collect::<Vec<_>>()
        });
        let unique: HashSet<_> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len());
        assert_eq!(unique.iter().min(), Some(&RuntimeId(1)));
        assert_eq!(unique.iter().max(), Some(&RuntimeId(ids.len())));
    }
}
