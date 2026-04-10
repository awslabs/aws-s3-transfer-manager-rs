/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Loom-backed wrappers that present the same API surface as the sibling `std`
//! module. Active only under `#[cfg(all(test, loom))]`.

// ---------------------------------------------------------------------------
// Mutex / MutexGuard / Condvar — thin wrappers that unwrap poisoned locks
// ---------------------------------------------------------------------------

pub(crate) use loom::sync::MutexGuard;

pub(crate) struct Mutex<T>(loom::sync::Mutex<T>);

impl<T> Mutex<T> {
    pub(crate) fn new(val: T) -> Self {
        Self(loom::sync::Mutex::new(val))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().unwrap()
    }
}

pub(crate) struct Condvar(loom::sync::Condvar);

impl Condvar {
    pub(crate) fn new() -> Self {
        Self(loom::sync::Condvar::new())
    }

    pub(crate) fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.0.wait(guard).unwrap()
    }

    pub(crate) fn notify_one(&self) {
        self.0.notify_one();
    }

    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }
}

// ---------------------------------------------------------------------------
// Re-export modules matching the std module layout
// ---------------------------------------------------------------------------

pub(crate) mod cell {
    pub(crate) use loom::cell::UnsafeCell;
}

pub(crate) mod sync {
    pub(crate) use loom::sync::Arc;

    pub(crate) use super::{Condvar, Mutex, MutexGuard};

    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{
            AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering,
        };
    }
}

pub(crate) mod thread {
    pub(crate) use loom::thread::{spawn, JoinHandle};
}
