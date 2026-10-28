/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Non-owning observation of managed-thread joins after owner release.

use crate::{RuntimeId, RuntimePartitionId, ShutdownError, identity};
use std::{
    io,
    sync::{Arc, Mutex},
    thread,
};
use tokio::sync::watch;

/// Observes shutdown completion without retaining runtime ownership.
///
/// Waiting does not request shutdown. The owner must be dropped or consumed by
/// [`crate::ManagedRuntime::shutdown`] before completion can occur. Retained
/// execution handles and completion observers do not prevent that request.
/// Clones and repeated waits observe the same join outcome.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShutdownCompletion {
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    id: RuntimeId,
    released: watch::Receiver<bool>,
    joining: Mutex<Joining>,
}

#[derive(Debug)]
enum Joining {
    Pending(Option<Vec<thread::JoinHandle<()>>>),
    Started(watch::Receiver<Option<Result<(), ShutdownError>>>),
}

pub(crate) fn completion(id: RuntimeId) -> (ShutdownCompletion, watch::Sender<bool>) {
    let (released, receiver) = watch::channel(false);
    let completion = ShutdownCompletion {
        state: Arc::new(State {
            id,
            released: receiver,
            joining: Mutex::new(Joining::Pending(None)),
        }),
    };
    (completion, released)
}

impl ShutdownCompletion {
    /// Waits for every managed thread to be joined after its owner is released.
    ///
    /// Success includes destruction of each thread's async runtime and
    /// thread-local values. Outstanding async tasks are cancelled; running
    /// blocking tasks must finish. Non-yielding or indefinitely blocked work
    /// can prevent completion. Independently retained HTTP clients or other
    /// caller-owned resources are outside this completion boundary.
    ///
    /// Joining runs on a separate thread and requires no ambient Tokio runtime.
    /// Dropping the wait future neither requests shutdown nor cancels joining
    /// that has already started. Another observer can await its outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when polled on this runtime's managed or blocking-pool
    /// threads, when the joining thread cannot start or loses its result, or
    /// when a managed thread panics. All managed threads are joined before a
    /// managed-thread panic is reported.
    pub async fn wait(&self) -> Result<(), ShutdownError> {
        self.wait_with(start).await
    }

    pub(crate) fn release(&self, threads: Vec<thread::JoinHandle<()>>) {
        let mut joining = self
            .state
            .joining
            .lock()
            .expect("shutdown state lock poisoned");
        let Joining::Pending(pending) = &mut *joining else {
            unreachable!("joining cannot start before owner release");
        };
        assert!(
            pending.is_none(),
            "runtime owner releases join handles once"
        );
        *pending = Some(threads);
    }

    async fn wait_with(
        &self,
        start: impl FnOnce(Job) -> io::Result<thread::JoinHandle<()>>,
    ) -> Result<(), ShutdownError> {
        let id = self.state.id;
        if identity::owns_current_thread(id) {
            return Err(ShutdownError::from_runtime(id));
        }
        let mut released = self.state.released.clone();
        released
            .wait_for(|ready| *ready)
            .await
            .map_err(|source| ShutdownError::completion(id, source))?;

        let (mut receiver, job) = {
            let mut joining = self
                .state
                .joining
                .lock()
                .expect("shutdown state lock poisoned");
            match &mut *joining {
                Joining::Started(receiver) => (receiver.clone(), None),
                Joining::Pending(threads) => {
                    let (publisher, receiver) = watch::channel(None);
                    let job = Job {
                        id,
                        threads: threads
                            .take()
                            .expect("released owner publishes join handles"),
                        completion: publisher.clone(),
                    };
                    *joining = Joining::Started(receiver.clone());
                    (receiver, Some((job, publisher)))
                }
            }
        };
        // Keep startup outside the state lock. Its failure is published to every
        // observer, while a lost joiner closes the channel without a result.
        if let Some((job, publisher)) = job {
            match start(job) {
                Ok(thread) => drop(thread),
                Err(source) => {
                    publisher.send_replace(Some(Err(ShutdownError::joiner(id, source))));
                }
            }
        }
        loop {
            let result = receiver.borrow_and_update().clone();
            if let Some(result) = result {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|source| ShutdownError::completion(id, source))?;
        }
    }
}

struct Job {
    id: RuntimeId,
    threads: Vec<thread::JoinHandle<()>>,
    completion: watch::Sender<Option<Result<(), ShutdownError>>>,
}

impl Job {
    fn run(self) {
        let mut result = Ok(());
        for (index, thread) in self.threads.into_iter().enumerate() {
            if let Err(payload) = thread.join()
                && result.is_ok()
            {
                result = Err(ShutdownError::thread_panic(
                    RuntimePartitionId::new(self.id, index),
                    payload,
                ));
            }
        }
        self.completion.send_replace(Some(result));
    }
}

fn start(job: Job) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("smithy-shutdown-{}", job.id.0))
        .spawn(move || job.run())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ShutdownErrorKind;
    use std::{
        error::Error,
        future::Future,
        sync::mpsc,
        task::{Context, Poll, Waker},
    };

    async fn join(
        id: RuntimeId,
        threads: Vec<thread::JoinHandle<()>>,
    ) -> Result<(), ShutdownError> {
        join_with(id, threads, start).await
    }

    async fn join_with(
        id: RuntimeId,
        threads: Vec<thread::JoinHandle<()>>,
        start: impl FnOnce(Job) -> io::Result<thread::JoinHandle<()>>,
    ) -> Result<(), ShutdownError> {
        let (observer, released) = completion(id);
        observer.release(threads);
        released.send_replace(true);
        observer.wait_with(start).await
    }

    fn caller() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    #[test]
    fn thread_panic_is_reported_after_all_threads_are_joined() {
        let id = RuntimeId::allocate().unwrap();
        let first = thread::spawn(|| panic!("first thread failed"));
        let second = thread::spawn(|| panic!("second thread failed"));
        let (release_tx, release) = mpsc::channel::<()>();
        let (finished_tx, finished) = mpsc::channel();
        let last = thread::spawn(move || {
            let _ = release.recv();
            finished_tx.send(()).unwrap();
        });
        let mut shutdown = Box::pin(join(id, vec![first, second, last]));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            shutdown.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(release_tx);
        let error = caller().block_on(shutdown).unwrap_err();
        assert_eq!(error.kind(), ShutdownErrorKind::ThreadPanic);
        assert!(error.to_string().contains("partition-0"));
        assert!(error.to_string().contains("first thread failed"));
        assert!(!error.to_string().contains("second thread failed"));
        finished.try_recv().unwrap();
    }

    #[test]
    fn joining_thread_creation_failure_preserves_source() {
        let id = RuntimeId::allocate().unwrap();
        let error = caller()
            .block_on(join_with(id, Vec::new(), |_| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "joining thread denied",
                ))
            }))
            .unwrap_err();
        assert_eq!(error.kind(), ShutdownErrorKind::Joiner);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied,
        );
        assert!(error.to_string().contains(&id.to_string()));
    }

    #[test]
    fn concurrent_observers_start_one_joiner() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let id = RuntimeId::allocate().unwrap();
        let (observer, released) = completion(id);
        observer.release(vec![thread::spawn(|| {})]);
        released.send_replace(true);
        let starts = Arc::new(AtomicUsize::new(0));
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let observer = observer.clone();
                let starts = starts.clone();
                thread::spawn(move || {
                    caller().block_on(observer.wait_with(move |job| {
                        starts.fetch_add(1, Ordering::SeqCst);
                        start(job)
                    }))
                })
            })
            .collect();
        for waiter in waiters {
            waiter.join().unwrap().unwrap();
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        caller().block_on(observer.wait()).unwrap();
    }

    #[test]
    fn repeated_waits_share_joiner_failure_and_source() {
        let id = RuntimeId::allocate().unwrap();
        let (observer, released) = completion(id);
        observer.release(Vec::new());
        released.send_replace(true);
        let first = caller()
            .block_on(observer.wait_with(|_| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "joiner denied",
                ))
            }))
            .unwrap_err();
        let second = caller()
            .block_on(observer.wait_with(|_| panic!("a failed join cannot start a second joiner")))
            .unwrap_err();
        assert_eq!(first.kind(), ShutdownErrorKind::Joiner);
        assert_eq!(second.kind(), first.kind());
        assert!(std::ptr::eq(
            first.source().unwrap(),
            second.source().unwrap()
        ));
    }

    #[test]
    fn lost_completion_is_reported() {
        let id = RuntimeId::allocate().unwrap();
        let result = caller().block_on(join_with(id, Vec::new(), |job| {
            thread::Builder::new().spawn(move || {
                drop(job);
                panic!("joining thread failed");
            })
        }));
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ShutdownErrorKind::Completion);
        assert!(error.to_string().contains("completion"));
        assert!(error.source().unwrap().is::<watch::error::RecvError>());
    }
}
