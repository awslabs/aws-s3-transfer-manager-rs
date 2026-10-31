/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Waker-driven future polling without an async runtime.

use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
};

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

/// Requests shutdown through the runtime adapter and joins owned threads.
///
/// Caller-owned Tokio runtimes remain the test fixture's responsibility.
pub(crate) async fn shutdown(handle: &crate::client::Handle) {
    let completion = handle.runtime.shutdown_completion();
    handle.runtime.shutdown();
    if let Some(completion) = completion {
        tokio::time::timeout(std::time::Duration::from_secs(5), completion.wait())
            .await
            .expect("managed threads did not finish test teardown")
            .expect("managed thread shutdown failed");
    }
}
