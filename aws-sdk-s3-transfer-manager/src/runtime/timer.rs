/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Client-scoped time primitives: sleep, deadlines, and the monotonic clock.
//!
//! Transfer-manager retry backoff and request deadlines wait through a
//! [`Timer`]. On `wasm32-unknown-unknown`, which has no tokio timer, the timer
//! uses the S3 client's [`AsyncSleep`](aws_sdk_s3::config::AsyncSleep)
//! implementation so the SDK and the transfer manager share one host timer. On
//! every other target it uses tokio's timer, which follows the tokio test clock
//! when it is paused. The timer also carries the client's [`Clock`].

use std::future::Future;
use std::time::Duration;

use super::Clock;

/// Time operations for one client.
#[derive(Clone, Debug)]
pub(crate) struct Timer {
    clock: Clock,
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    sleep_impl: aws_sdk_s3::config::SharedAsyncSleep,
}

/// The error returned by [`Timer::timeout`] when the deadline elapses first.
#[derive(Debug)]
pub(crate) struct Elapsed;

impl Timer {
    /// Returns the timer for a client that reads `clock` and sends requests
    /// through `client`.
    ///
    /// # Panics
    ///
    /// On `wasm32-unknown-unknown`, panics if `client` has no
    /// [`AsyncSleep`](aws_sdk_s3::config::AsyncSleep) implementation.
    pub(crate) fn new(clock: Clock, client: &aws_sdk_s3::Client) -> Self {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        {
            let _ = client;
            Self { clock }
        }
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        {
            let sleep_impl = client.config().sleep_impl().expect(
                "wasm32-unknown-unknown requires an AsyncSleep implementation in the S3 \
                 configuration",
            );
            Self { clock, sleep_impl }
        }
    }

    /// Returns a timer backed by tokio's timer and the system clock.
    #[cfg(all(test, not(all(target_family = "wasm", target_os = "unknown"))))]
    pub(crate) fn tokio() -> Self {
        Self {
            clock: Clock::system(),
        }
    }

    /// Returns the client's monotonic clock.
    pub(crate) fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Completes after `duration` has elapsed.
    pub(crate) async fn sleep(&self, duration: Duration) {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        tokio::time::sleep(duration).await;
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        aws_sdk_s3::config::AsyncSleep::sleep(&self.sleep_impl, duration).await;
    }

    /// Runs `future` until it completes or `duration` elapses.
    ///
    /// `future` is polled before the deadline on each wake, so a future that is
    /// ready when the deadline elapses completes. When the deadline wins,
    /// `future` is dropped before this returns [`Elapsed`].
    pub(crate) async fn timeout<F>(
        &self,
        duration: Duration,
        future: F,
    ) -> Result<F::Output, Elapsed>
    where
        F: Future,
    {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        {
            tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Elapsed)
        }
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        {
            tokio::select! {
                biased;
                output = future => Ok(output),
                () = self.sleep(duration) => Err(Elapsed),
            }
        }
    }
}
