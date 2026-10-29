/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_execution::{ManagedRuntime, ShutdownErrorKind, topology::Topology};
use std::{
    future::{self, Future},
    io::Write,
    net::TcpListener,
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn caller() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn runtime() -> ManagedRuntime {
    ManagedRuntime::builder()
        .topology(Topology::detect().unwrap().with_max_placements(2))
        .pin_threads(false)
        .build()
        .unwrap()
}

#[test]
fn cross_thread_tasks_stay_on_their_partition_and_scope_current_identity() {
    let first = runtime();
    let second = runtime();
    let execution = first.handle();
    let foreign = second.handle();
    assert_ne!(execution.id(), foreign.id());
    assert_ne!(execution.partitions()[0].id(), foreign.partitions()[0].id());
    assert!(execution.current_partition().is_none());
    let caller_id = thread::current().id();
    let awaiting = caller();
    let mut threads = std::collections::HashSet::new();
    assert_eq!(
        execution.partitions().len(),
        execution.topology().placements().len()
    );
    for (index, partition) in execution.partitions().iter().enumerate() {
        assert_eq!(partition.id().index(), index);
        assert_eq!(partition.id().runtime_id(), execution.id());
        assert_eq!(
            partition.placement(),
            &execution.topology().placements()[index]
        );
        assert_eq!(partition.bound_processor(), None);
        let local = execution.clone();
        let other = foreign.clone();
        let expected = partition.id();
        let thread = awaiting
            .block_on(partition.spawn(async move {
                assert_eq!(local.current_partition().unwrap().id(), expected);
                assert!(other.current_partition().is_none());
                let before = thread::current().id();
                tokio::time::sleep(Duration::from_millis(1)).await;
                assert_eq!(thread::current().id(), before);
                assert_eq!(local.current_partition().unwrap().id(), expected);
                (before, 42)
            }))
            .unwrap();
        assert_ne!(thread.0, caller_id);
        assert_eq!(thread.1, 42);
        threads.insert(thread.0);
    }
    assert_eq!(threads.len(), execution.partitions().len());
}

#[test]
fn thread_drives_socket_io_and_timers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection.write_all(b"ready").unwrap();
    });
    let runtime = runtime();
    let task = runtime.handle().partitions()[0].spawn(async move {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut received = [0; 5];
        let mut used = 0;
        while used < received.len() {
            stream.readable().await.unwrap();
            match stream.try_read(&mut received[used..]) {
                Ok(0) => panic!("peer closed before delivering its message"),
                Ok(count) => used += count,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("socket read failed: {error}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        received
    });
    assert_eq!(
        caller().block_on(async { tokio::time::timeout(TIMEOUT, task).await.unwrap().unwrap() }),
        *b"ready"
    );
    server.join().unwrap();
}

struct DropSignal(mpsc::Sender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

thread_local! {
    static THREAD_EXIT: std::cell::RefCell<Option<DropSignal>> = const { std::cell::RefCell::new(None) };
}

#[test]
fn retained_handles_do_not_keep_threads_running_after_owner_drop() {
    let runtime = runtime();
    let execution = runtime.handle();
    let mut pending = Vec::new();
    let mut dropped = Vec::new();
    let mut retained = Vec::new();
    for partition in execution.partitions() {
        let partition = partition.clone();
        retained.push(partition.clone());
        let (started_tx, started) = mpsc::channel();
        let (drop_tx, drop_rx) = mpsc::channel();
        pending.push(partition.spawn(async move {
            let _signal = DropSignal(drop_tx);
            started_tx.send(()).unwrap();
            future::pending::<()>().await;
        }));
        started.recv_timeout(TIMEOUT).unwrap();
        dropped.push(drop_rx);
    }
    drop(runtime);
    let awaiting = caller();
    for (task, dropped) in pending.into_iter().zip(dropped) {
        dropped.recv_timeout(TIMEOUT).unwrap();
        assert!(awaiting.block_on(task).unwrap_err().is_cancelled());
    }
    assert!(!execution.partitions().is_empty());
    for partition in retained {
        assert!(
            awaiting
                .block_on(partition.spawn(async {}))
                .unwrap_err()
                .is_cancelled()
        );
    }
}

#[test]
fn owner_can_be_dropped_on_its_own_thread() {
    let runtime = runtime();
    let partition = runtime.handle().partitions()[0].clone();
    let task = partition.spawn(async move {
        drop(runtime);
        42
    });
    assert_eq!(
        caller().block_on(async { tokio::time::timeout(TIMEOUT, task).await.unwrap().unwrap() }),
        42
    );
}

#[test]
fn owner_drop_does_not_wait_for_running_blocking_tasks() {
    let runtime = runtime();
    let partition = runtime.handle().partitions()[0].clone();
    let (started_tx, started) = mpsc::channel();
    let (release_tx, release) = mpsc::channel::<()>();
    let (finished_tx, finished) = mpsc::channel();
    let awaiting = caller();
    let task = partition.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let _finished = DropSignal(finished_tx);
            started_tx.send(()).unwrap();
            let _ = release.recv();
        })
        .await
        .unwrap();
    });
    started.recv_timeout(TIMEOUT).unwrap();

    let (dropped_tx, dropped) = mpsc::channel();
    let dropping = thread::spawn(move || {
        drop(runtime);
        dropped_tx.send(()).unwrap();
    });
    let result = dropped.recv_timeout(TIMEOUT);
    // Release the blocking task even if Drop incorrectly waits for it.
    drop(release_tx);
    dropping.join().unwrap();
    result.unwrap();
    finished.recv_timeout(TIMEOUT).unwrap();
    if let Err(error) = awaiting.block_on(task) {
        assert!(error.is_cancelled());
    }
}

#[test]
fn task_panic_is_reported_without_killing_thread() {
    let runtime = runtime();
    let partition = runtime.handle().partitions()[0].clone();
    let awaiting = caller();
    let error = awaiting
        .block_on(partition.spawn(async { panic!("application task failed") }))
        .unwrap_err();
    assert!(error.is_panic());
    assert_eq!(
        awaiting.block_on(partition.spawn(async { 42 })).unwrap(),
        42
    );
    awaiting.block_on(runtime.shutdown()).unwrap();
}

#[test]
fn partition_targets_thread_and_blocking_threads_have_no_partition_identity() {
    let runtime = ManagedRuntime::builder()
        .topology(Topology::detect().unwrap().with_max_placements(1))
        .thread_name_prefix("thread-test")
        .pin_threads(false)
        .build()
        .unwrap();
    let execution = runtime.handle();
    let partition = &execution.partitions()[0];
    let local = execution.clone();
    let expected = partition.id();
    let awaiting = caller();
    let outside = execution.clone();
    let task = partition.spawn(async move {
        assert_eq!(local.current_partition().unwrap().id(), expected);
        let name = thread::current().name().unwrap().to_owned();
        let blocking_is_outside =
            tokio::task::spawn_blocking(move || outside.current_partition().is_none())
                .await
                .unwrap();
        (name, blocking_is_outside)
    });
    let (name, blocking_is_outside) = awaiting.block_on(task).unwrap();
    assert!(name.starts_with("thread-test-"));
    assert!(blocking_is_outside);
}

#[test]
fn async_shutdown_joins_threads_and_cancels_outstanding_tasks() {
    let runtime = runtime();
    let execution = runtime.handle();
    let mut tasks = Vec::new();
    let mut task_drops = Vec::new();
    let mut thread_exits = Vec::new();
    for partition in execution.partitions() {
        let (started_tx, started) = mpsc::channel();
        let (drop_tx, dropped) = mpsc::channel();
        let (exit_tx, exited) = mpsc::channel();
        tasks.push(partition.spawn(async move {
            let _dropped = DropSignal(drop_tx);
            THREAD_EXIT.with(|exit| *exit.borrow_mut() = Some(DropSignal(exit_tx)));
            started_tx.send(()).unwrap();
            future::pending::<()>().await;
        }));
        started.recv_timeout(TIMEOUT).unwrap();
        task_drops.push(dropped);
        thread_exits.push(exited);
    }
    let awaiting = caller();
    awaiting.block_on(runtime.shutdown()).unwrap();
    // Thread-local destruction happens after the managed thread's main body.
    // Its notifications must already be available when shutdown completes.
    for exited in thread_exits {
        exited.try_recv().unwrap();
    }
    for (task, dropped) in tasks.into_iter().zip(task_drops) {
        dropped.try_recv().unwrap();
        assert!(awaiting.block_on(task).unwrap_err().is_cancelled());
    }
    for partition in execution.partitions() {
        assert!(
            awaiting
                .block_on(partition.spawn(async {}))
                .unwrap_err()
                .is_cancelled()
        );
    }
}

#[test]
fn async_shutdown_waits_for_blocking_tasks_without_blocking_caller() {
    let runtime = runtime();
    let partition = runtime.handle().partitions()[0].clone();
    let (started_tx, started) = mpsc::channel();
    let (release_tx, release) = mpsc::channel::<()>();
    let (progress_tx, progress) = mpsc::channel();
    let task = partition.spawn(async move {
        tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            let _ = release.recv();
        })
        .await
        .unwrap();
    });
    started.recv_timeout(TIMEOUT).unwrap();
    let releasing = thread::spawn(move || {
        // Release on failure too, so an incorrect blocking implementation cannot
        // hang the test process indefinitely.
        let progressed = progress.recv_timeout(TIMEOUT).is_ok();
        drop(release_tx);
        progressed
    });
    let mut shutdown = Box::pin(runtime.shutdown());
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        shutdown.as_mut().poll(&mut context),
        Poll::Pending
    ));
    let awaiting = caller();
    awaiting.block_on(async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        progress_tx.send(()).unwrap();
        tokio::time::timeout(TIMEOUT, shutdown)
            .await
            .unwrap()
            .unwrap();
    });
    assert!(releasing.join().unwrap());
    if let Err(error) = awaiting.block_on(task) {
        assert!(error.is_cancelled());
    }
}

#[test]
fn shutdown_from_managed_thread_returns_error() {
    let runtime = runtime();
    let id = runtime.handle().id();
    let partition = runtime.handle().partitions()[0].clone();
    let task = partition.spawn(async move { runtime.shutdown().await });
    let error = caller()
        .block_on(async { tokio::time::timeout(TIMEOUT, task).await.unwrap().unwrap() })
        .unwrap_err();
    assert_eq!(error.kind(), ShutdownErrorKind::FromRuntime);
    assert!(error.to_string().contains("thread it owns"));
    assert!(error.to_string().contains(&id.to_string()));
}

#[test]
fn shutdown_from_own_blocking_thread_rejects_foreign_tokio_context() {
    let runtime = runtime();
    let partition = runtime.handle().partitions()[0].clone();
    let (result_tx, result) = mpsc::channel();
    let task = partition.spawn(async move {
        let _ = tokio::task::spawn_blocking(move || {
            let foreign = caller();
            let result = foreign.block_on(async {
                tokio::time::timeout(TIMEOUT, runtime.shutdown())
                    .await
                    .unwrap()
            });
            result_tx.send(result).unwrap();
        })
        .await;
    });
    let error = result.recv_timeout(TIMEOUT).unwrap().unwrap_err();
    assert_eq!(error.kind(), ShutdownErrorKind::FromRuntime);
    assert!(error.to_string().contains("thread it owns"));
    if let Err(error) = caller().block_on(task) {
        assert!(error.is_cancelled());
    }
}

#[test]
fn dropping_shutdown_future_keeps_shutdown_requested() {
    for poll_first in [false, true] {
        let runtime = runtime();
        let execution = runtime.handle();
        let partition = execution.partitions()[0].clone();
        let (started_tx, started) = mpsc::channel();
        let (release_tx, release) = mpsc::channel::<()>();
        let (exit_tx, exited) = mpsc::channel();
        let task = partition.spawn(async move {
            THREAD_EXIT.with(|exit| *exit.borrow_mut() = Some(DropSignal(exit_tx)));
            tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                let _ = release.recv();
            })
            .await
            .unwrap();
        });
        started.recv_timeout(TIMEOUT).unwrap();
        let mut shutdown = Box::pin(runtime.shutdown());
        if poll_first {
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                shutdown.as_mut().poll(&mut context),
                Poll::Pending
            ));
        }
        drop(shutdown);
        drop(release_tx);
        exited.recv_timeout(TIMEOUT).unwrap();
        let awaiting = caller();
        if let Err(error) = awaiting.block_on(task) {
            assert!(error.is_cancelled());
        }
        assert!(
            awaiting
                .block_on(partition.spawn(async {}))
                .unwrap_err()
                .is_cancelled()
        );
    }
}

#[test]
fn shutdown_can_be_driven_without_tokio_context() {
    struct Notify(mpsc::Sender<()>);
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    assert!(tokio::runtime::Handle::try_current().is_err());
    let (notify, notified) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notify(notify)));
    let mut context = Context::from_waker(&waker);
    let mut shutdown = Box::pin(runtime().shutdown());
    loop {
        match shutdown.as_mut().poll(&mut context) {
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
            Poll::Pending => notified.recv_timeout(TIMEOUT).unwrap(),
        }
    }
}

#[cfg(all(any(target_os = "linux", target_os = "windows"), not(miri)))]
#[test]
fn default_pinning_binds_identified_targets() {
    let runtime = ManagedRuntime::builder()
        .topology(Topology::detect().unwrap().with_max_placements(2))
        .build()
        .unwrap();
    let awaiting = caller();
    for partition in runtime.handle().partitions() {
        let expected = partition.placement().logical_processor().unwrap();
        assert_eq!(partition.bound_processor(), Some(expected));
        let processors = awaiting
            .block_on(
                partition
                    .spawn(async { Topology::detect().unwrap().logical_processors().to_vec() }),
            )
            .unwrap();
        assert_eq!(processors.len(), 1);
        assert_eq!(processors[0].id(), expected);
    }
}

#[cfg(feature = "dial9")]
mod tracing {
    use super::*;
    use dial9::{
        Dial9Handle, MemoryBuffer,
        core::pipeline::{ProcessError, SegmentData, SegmentProcessor},
        format::{Decoder, types::FieldValueRef},
        recorder,
    };
    use std::{collections::HashMap, error::Error, pin::Pin, sync::Mutex};

    struct Capture(Arc<Mutex<Vec<Vec<u8>>>>);

    impl SegmentProcessor for Capture {
        fn name(&self) -> &'static str {
            "capture"
        }

        fn process(
            &mut self,
            data: SegmentData,
        ) -> Pin<Box<dyn Future<Output = Result<SegmentData, ProcessError>> + Send + '_>> {
            self.0
                .lock()
                .unwrap()
                .push(data.payload().clone().into_vec());
            Box::pin(async move { Ok(data) })
        }
    }

    fn traced_runtime(handle: Dial9Handle, prefix: &str) -> ManagedRuntime {
        ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(2))
            .pin_threads(false)
            .thread_name_prefix(prefix)
            .dial9_handle(handle)
            .build()
            .unwrap()
    }

    fn rejects_blocking_self_wait(runtime: ManagedRuntime) {
        let partition = runtime.handle().partitions()[0].clone();
        let (result_tx, result) = mpsc::channel();
        let task = partition.spawn(async move {
            let _ = tokio::task::spawn_blocking(move || {
                // Physical ownership must survive entering a foreign runtime,
                // including when Dial9 has replaced the native thread callbacks.
                let result = caller()
                    .block_on(async { tokio::time::timeout(TIMEOUT, runtime.shutdown()).await });
                result_tx.send(result).unwrap();
            })
            .await;
        });
        let error = result.recv_timeout(TIMEOUT).unwrap().unwrap().unwrap_err();
        assert_eq!(error.kind(), ShutdownErrorKind::FromRuntime);
        assert!(error.to_string().contains("thread it owns"));
        if let Err(error) = caller().block_on(task) {
            assert!(error.is_cancelled());
        }
    }

    #[test]
    fn disabled_attachment_preserves_plain_execution_and_shutdown_protection() {
        let mut builder = ManagedRuntime::builder();
        builder.set_dial9_handle(Some(Dial9Handle::disabled()));
        builder.set_dial9_handle(None);
        let runtime = builder
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .pin_threads(false)
            .build()
            .unwrap();
        let plain = runtime.handle().partitions()[0]
            .spawn(async { Dial9Handle::try_current_thread().is_none() });
        assert!(caller().block_on(plain).unwrap());
        caller().block_on(runtime.shutdown()).unwrap();
        rejects_blocking_self_wait(traced_runtime(Dial9Handle::disabled(), "disabled"));
    }

    #[test]
    fn live_attachment_traces_target_partitions_and_preserves_shutdown_protection() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let recorder = recorder(MemoryBuffer::new(1 << 20).unwrap())
            .pipe(Capture(captured.clone()))
            .build();
        let handle = recorder.handle().clone();
        let awaiting = caller();
        let mut names = Vec::new();
        for _ in 0..2 {
            // The same prefix and recorder must still distinguish runtimes and
            // their partitions. Submissions come from this untraced caller.
            let runtime = traced_runtime(handle.clone(), "trace");
            let execution = runtime.handle();
            for partition in execution.partitions() {
                let local = execution.clone();
                let expected = partition.id();
                names.push(
                    awaiting
                        .block_on(partition.spawn(async move {
                            assert!(Dial9Handle::try_current_thread().unwrap().is_enabled());
                            assert_eq!(local.current_partition().unwrap().id(), expected);
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            thread::current().name().unwrap().to_owned()
                        }))
                        .unwrap(),
                );
            }
            awaiting.block_on(runtime.shutdown()).unwrap();
            assert!(
                awaiting
                    .block_on(execution.partitions()[0].spawn(async {}))
                    .unwrap_err()
                    .is_cancelled()
            );
        }
        rejects_blocking_self_wait(traced_runtime(handle.clone(), "self-wait"));
        recorder.graceful_shutdown(TIMEOUT);

        let mut metadata = HashMap::new();
        let mut polled = Vec::new();
        let mut wakes = 0;
        for bytes in captured.lock().unwrap().iter() {
            let mut decoder = Decoder::new(bytes).expect("sealed trace header");
            decoder
                .for_each_event(|event| {
                    if event.name == "SegmentMetadataEvent" {
                        if let [FieldValueRef::StringMap(entries)] = event.fields {
                            metadata
                                .extend(entries.iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
                        }
                    } else if event.name == "PollStartEvent" {
                        let mut worker = None;
                        let mut location = None;
                        for (name, value) in event.field_names().zip(event.fields) {
                            match (name, value) {
                                ("worker_id", FieldValueRef::Varint(id)) => worker = Some(*id),
                                ("spawn_loc", FieldValueRef::PooledString(id)) => {
                                    location = event.string_pool.get(*id);
                                }
                                _ => {}
                            }
                        }
                        if let (Some(worker), Some(location)) = (worker, location) {
                            polled.push((worker, location.to_owned()));
                        }
                    } else if event.name == "WakeEventEvent" {
                        wakes += 1;
                    }
                })
                .unwrap();
        }
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
        for name in names {
            let workers = metadata
                .get(&format!("runtime.{name}"))
                .expect("partition in trace");
            // A trace from only the runtime's stop future cannot satisfy this:
            // each target must contain polls of the externally submitted task.
            assert!(
                workers
                    .split(',')
                    .any(|id| polled.iter().any(|(worker, location)| {
                        id == worker.to_string() && location.contains("smoke.rs")
                    })),
                "no consumer task polls recorded for {name}"
            );
        }
        assert!(wakes > 0);

        let error = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(2))
            .pin_threads(false)
            .dial9_handle(handle)
            .build()
            .unwrap_err();
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert!(error.source().unwrap().to_string().contains("shut down"));
    }
}

#[test]
fn each_partition_limits_blocking_execution_to_one_thread() {
    let runtime = runtime();
    let awaiting = caller();
    for partition in runtime.handle().partitions() {
        let (started_tx, started) = mpsc::channel();
        let (release_tx, release) = mpsc::channel::<()>();
        let (queued_tx, queued) = mpsc::channel();
        let (second_tx, second) = mpsc::channel();
        let task = partition.spawn(async move {
            let first = tokio::task::spawn_blocking(move || {
                started_tx.send(thread::current().id()).unwrap();
                let _ = release.recv();
            });
            let next = tokio::task::spawn_blocking(move || {
                second_tx.send(thread::current().id()).unwrap();
            });
            queued_tx.send(()).unwrap();
            first.await.unwrap();
            next.await.unwrap();
        });
        let first_thread = started.recv_timeout(TIMEOUT).unwrap();
        queued.recv_timeout(TIMEOUT).unwrap();
        let early = second.recv_timeout(Duration::from_millis(50));
        // Always release the first task, including on failure.
        drop(release_tx);
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(second.recv_timeout(TIMEOUT).unwrap(), first_thread);
        awaiting.block_on(task).unwrap();
    }
}
