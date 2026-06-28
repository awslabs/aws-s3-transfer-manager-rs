/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Production wrappers that present a loom-compatible API over `std` and
//! `parking_lot` primitives. Under `cfg(s3_tm_loom)` the sibling `loom` module
//! provides the same API surface backed by `loom` types.
//!
//! Under `cfg(miri)`, Mutex/Condvar fall back to `std::sync` because
//! `parking_lot` uses integer-to-pointer casts that violate strict provenance.

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

// ---------------------------------------------------------------------------
// UnsafeCell — closure-based API matching loom::cell::UnsafeCell
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

impl<T> UnsafeCell<T> {
    pub(crate) fn new(data: T) -> Self {
        Self(std::cell::UnsafeCell::new(data))
    }

    #[inline]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    #[inline]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

// ---------------------------------------------------------------------------
// Mutex / MutexGuard
//
// Production: parking_lot (faster Condvar, no poisoning).
// Miri: std::sync (parking_lot uses integer-to-pointer casts that violate
//        strict provenance — that's a parking_lot concern, not ours).
// ---------------------------------------------------------------------------

#[cfg(not(miri))]
pub(crate) struct Mutex<T>(PhantomData<std::sync::Mutex<T>>, parking_lot::Mutex<T>);

#[cfg(not(miri))]
pub(crate) struct MutexGuard<'a, T>(
    PhantomData<std::sync::MutexGuard<'a, T>>,
    parking_lot::MutexGuard<'a, T>,
);

#[cfg(not(miri))]
impl<T> Mutex<T> {
    pub(crate) fn new(val: T) -> Self {
        Self(PhantomData, parking_lot::Mutex::new(val))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard(PhantomData, self.1.lock())
    }
}

#[cfg(not(miri))]
impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.1
    }
}

#[cfg(not(miri))]
impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.1
    }
}

#[cfg(miri)]
pub(crate) struct Mutex<T>(std::sync::Mutex<T>);

#[cfg(miri)]
pub(crate) struct MutexGuard<'a, T>(std::sync::MutexGuard<'a, T>);

#[cfg(miri)]
impl<T> Mutex<T> {
    pub(crate) fn new(val: T) -> Self {
        Self(std::sync::Mutex::new(val))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard(self.0.lock().expect("poisoned"))
    }
}

#[cfg(miri)]
impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(miri)]
impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

// ---------------------------------------------------------------------------
// Condvar — ownership-based wait matching loom/std (not parking_lot's &mut)
// ---------------------------------------------------------------------------

#[cfg(not(miri))]
pub(crate) struct Condvar(PhantomData<std::sync::Condvar>, parking_lot::Condvar);

#[cfg(not(miri))]
impl Condvar {
    pub(crate) fn new() -> Self {
        Self(PhantomData, parking_lot::Condvar::new())
    }

    pub(crate) fn wait<'a, T>(&self, mut guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.1.wait(&mut guard.1);
        guard
    }

    pub(crate) fn notify_one(&self) {
        self.1.notify_one();
    }

    pub(crate) fn notify_all(&self) {
        self.1.notify_all();
    }
}

#[cfg(miri)]
pub(crate) struct Condvar(std::sync::Condvar);

#[cfg(miri)]
impl Condvar {
    pub(crate) fn new() -> Self {
        Self(std::sync::Condvar::new())
    }

    pub(crate) fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        MutexGuard(self.0.wait(guard.0).expect("poisoned"))
    }

    pub(crate) fn notify_one(&self) {
        self.0.notify_one();
    }

    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }
}

// ---------------------------------------------------------------------------
// Re-export modules matching the loom module layout
// ---------------------------------------------------------------------------

pub(crate) mod cell {
    pub(crate) use super::UnsafeCell;
}

pub(crate) mod sync {
    pub(crate) use std::sync::Arc;

    pub(crate) use super::{Condvar, Mutex, MutexGuard};

    pub(crate) mod atomic {
        pub(crate) use std::sync::atomic::{
            AtomicBool, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering,
        };
    }
}

pub(crate) mod thread {
    pub(crate) use std::thread::{spawn, JoinHandle};
}
