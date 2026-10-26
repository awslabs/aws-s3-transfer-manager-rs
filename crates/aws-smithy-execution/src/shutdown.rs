/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{RuntimeId, RuntimePartitionId, ShutdownError};
use std::{io, thread};
use tokio::sync::oneshot;

pub(crate) async fn join(
    id: RuntimeId,
    threads: Vec<thread::JoinHandle<()>>,
) -> Result<(), ShutdownError> {
    join_with(id, threads, start).await
}

struct Job {
    id: RuntimeId,
    threads: Vec<thread::JoinHandle<()>>,
    completion: oneshot::Sender<Result<(), ShutdownError>>,
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
        // Cancellation only discards the result; every thread has been joined.
        let _ = self.completion.send(result);
    }
}

fn start(job: Job) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("smithy-shutdown-{}", job.id.0))
        .spawn(move || job.run())
}

async fn join_with(
    id: RuntimeId,
    threads: Vec<thread::JoinHandle<()>>,
    start: impl FnOnce(Job) -> io::Result<thread::JoinHandle<()>>,
) -> Result<(), ShutdownError> {
    let (completion, receiver) = oneshot::channel();
    let job = Job {
        id,
        threads,
        completion,
    };
    drop(start(job).map_err(|source| ShutdownError::joiner(id, source))?);
    receiver
        .await
        .map_err(|source| ShutdownError::completion(id, source))?
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
        assert!(error.source().unwrap().is::<oneshot::error::RecvError>());
    }
}
