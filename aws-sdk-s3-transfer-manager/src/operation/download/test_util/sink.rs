/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Scripted destination faults.
//!
//! A [`ScriptedSinkFactory`] wraps [`FileSinkFactory`], so bytes still reach the
//! real file, but each destination operation first consults a shared
//! [`WriteScript`]. A rule can hold the operation until the test releases it,
//! fail it, or delay it. Destination writes run synchronously inside a work
//! item's `execute`, so a held operation blocks its thread on a std `Condvar`.

use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Buf;

use crate::operation::download::body::DiskWriteCursor;
use crate::operation::download::sink::{FileSinkFactory, SinkFactory, SinkWrite};

/// Which destination operation a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Match {
    /// The n-th `write_all_at` through sinks sharing the script, counting
    /// from 1.
    NthWrite(usize),
    /// The first `write_all_at` whose destination position is at or after
    /// this offset.
    WriteAtOrAfter(u64),
    /// The resize in `finalize` that establishes the destination length.
    Finalize,
}

/// What a matched operation does before it reaches the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Block the calling thread until the test releases it through
    /// [`Held`].
    Hold,
    /// Fail with this kind without touching the file.
    Fail(io::ErrorKind),
    /// Sleep for this long, then proceed.
    Delay(Duration),
}

/// One destination operation, as logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    /// A positioned write of `len` bytes at destination position `pos`.
    Write { pos: u64, len: usize },
    /// The resize to `len` bytes that finalizes the destination.
    Finalize { len: u64 },
}

/// One entry in a script's log, in the order the script observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    /// The operation was entered. A matched action runs after this entry.
    Started(Op),
    /// The operation returned, with the error kind if it failed.
    Finished(Op, Result<(), io::ErrorKind>),
    /// A marker recorded by the test with [`WriteScript::mark`].
    Mark(&'static str),
}

/// Counted fault rules and an operation log, shared by every sink one
/// [`ScriptedSinkFactory`] creates.
///
/// Each rule fires at most once, on the first operation it matches. Rules
/// are checked in the order they were added. Nothing is random.
#[derive(Debug, Default)]
pub(crate) struct WriteScript {
    state: Mutex<ScriptState>,
    /// Signalled when a hold starts or is released.
    changed: Condvar,
}

/// Everything a [`WriteScript`] guards with its one lock, so a rule match, the
/// log entry, and a hold are recorded atomically with respect to each other.
#[derive(Debug, Default)]
struct ScriptState {
    /// Rules that have not fired yet.
    rules: Vec<(Match, Action)>,
    /// `write_all_at` calls seen so far.
    writes: usize,
    /// Events in the order the script observed them.
    log: Vec<Event>,
    /// Every hold that has started, indexed by hold id.
    holds: Vec<HoldSlot>,
}

/// One operation blocked by [`Action::Hold`], and how the test released it.
#[derive(Debug)]
struct HoldSlot {
    op: Op,
    /// Whether [`WriteScript::wait_held`] has handed this hold to a test.
    claimed: bool,
    /// Set by the test; the held operation proceeds once this is `Some`.
    release: Option<Result<(), io::ErrorKind>>,
}

impl ScriptState {
    /// Removes and returns the first rule matching `op`, the
    /// `write_number`-th write when `op` is a write.
    fn take_rule(&mut self, op: Op, write_number: usize) -> Option<Action> {
        let index = self.rules.iter().position(|(m, _)| match (*m, op) {
            (Match::NthWrite(n), Op::Write { .. }) => n == write_number,
            (Match::WriteAtOrAfter(offset), Op::Write { pos, .. }) => pos >= offset,
            (Match::Finalize, Op::Finalize { .. }) => true,
            _ => false,
        })?;
        Some(self.rules.remove(index).1)
    }
}

/// The error a scripted failure returns, distinguishable from a real one by
/// its message.
fn scripted_error(kind: io::ErrorKind) -> io::Error {
    io::Error::new(kind, "scripted destination failure")
}

impl WriteScript {
    /// Adds a rule and returns the script, so rules can be chained from
    /// construction.
    pub(crate) fn on(self: &Arc<Self>, m: Match, a: Action) -> Arc<Self> {
        self.lock().rules.push((m, a));
        Arc::clone(self)
    }

    /// Blocks until an operation not yet returned by an earlier call is held,
    /// then returns the guard that releases it.
    ///
    /// Panics if no operation is held within `timeout`.
    pub(crate) fn wait_held(self: &Arc<Self>, timeout: Duration) -> Held {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            if let Some(id) = state.holds.iter().position(|hold| !hold.claimed) {
                state.holds[id].claimed = true;
                return Held {
                    script: Arc::clone(self),
                    id,
                    op: state.holds[id].op,
                    released: false,
                };
            }
            let now = Instant::now();
            assert!(
                now < deadline,
                "no destination operation was held within {timeout:?}"
            );
            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Appends a marker to the log, ordering a test step against destination
    /// operations.
    pub(crate) fn mark(&self, label: &'static str) {
        self.lock().log.push(Event::Mark(label));
    }

    /// The log so far.
    pub(crate) fn events(&self) -> Vec<Event> {
        self.lock().log.clone()
    }

    /// Logs `op`, applies the first matching rule, runs `io` unless the rule
    /// failed the operation, and logs the result.
    fn run(&self, op: Op, io: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let result = self.before(op).and_then(|()| io());
        self.lock().log.push(Event::Finished(
            op,
            result.as_ref().map(|_| ()).map_err(io::Error::kind),
        ));
        result
    }

    /// Logs the start of `op` and applies the first rule it matches: returns a
    /// scripted failure, sleeps, or blocks until a [`Held`] releases it.
    fn before(&self, op: Op) -> io::Result<()> {
        let mut state = self.lock();
        state.log.push(Event::Started(op));
        if let Op::Write { .. } = op {
            state.writes += 1;
        }
        let write_number = state.writes;
        match state.take_rule(op, write_number) {
            None => Ok(()),
            Some(Action::Fail(kind)) => Err(scripted_error(kind)),
            Some(Action::Delay(delay)) => {
                drop(state);
                std::thread::sleep(delay);
                Ok(())
            }
            Some(Action::Hold) => {
                let id = state.holds.len();
                state.holds.push(HoldSlot {
                    op,
                    claimed: false,
                    release: None,
                });
                self.changed.notify_all();
                loop {
                    if let Some(release) = state.holds[id].release {
                        return release.map_err(scripted_error);
                    }
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    /// Locks the script state. A panicking test thread must not cascade into
    /// writers blocked on the script, so poisoning is ignored.
    fn lock(&self) -> MutexGuard<'_, ScriptState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A destination operation blocked by [`Action::Hold`].
///
/// Dropping the guard without releasing it lets the operation proceed, so a
/// failing test does not leave a writer blocked.
#[derive(Debug)]
pub(crate) struct Held {
    script: Arc<WriteScript>,
    id: usize,
    op: Op,
    released: bool,
}

impl Held {
    /// The held operation.
    pub(crate) fn op(&self) -> Op {
        self.op
    }

    /// Lets the held operation proceed to the file.
    pub(crate) fn release(mut self) {
        self.finish(Ok(()));
    }

    /// Fails the held operation with `kind` without touching the file.
    pub(crate) fn release_err(mut self, kind: io::ErrorKind) {
        self.finish(Err(kind));
    }

    fn finish(&mut self, release: Result<(), io::ErrorKind>) {
        self.released = true;
        self.script.lock().holds[self.id].release = Some(release);
        self.script.changed.notify_all();
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if !self.released {
            self.finish(Ok(()));
        }
    }
}

/// [`SinkFactory`] whose sinks consult a [`WriteScript`] before each write
/// and the finalizing resize, then delegate to a [`FileSinkFactory`] sink.
#[derive(Debug)]
pub(crate) struct ScriptedSinkFactory(pub(crate) Arc<WriteScript>);

impl SinkFactory for ScriptedSinkFactory {
    fn create(&self, file: std::fs::File, owns_file: bool) -> Box<dyn SinkWrite> {
        Box::new(ScriptedSink {
            inner: FileSinkFactory.create(file, owns_file),
            script: Arc::clone(&self.0),
        })
    }
}

/// File sink whose writes and finalization pass through a [`WriteScript`].
#[derive(Debug)]
struct ScriptedSink {
    inner: Box<dyn SinkWrite>,
    script: Arc<WriteScript>,
}

impl SinkWrite for ScriptedSink {
    fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> io::Result<()> {
        let op = Op::Write {
            pos,
            len: buf.remaining(),
        };
        self.script.run(op, || self.inner.write_all_at(buf, pos))
    }

    fn prepare(&self, expected_download_len: u64) -> io::Result<()> {
        self.inner.prepare(expected_download_len)
    }

    fn finalize(&self, expected_download_len: u64) -> io::Result<()> {
        let op = Op::Finalize {
            len: expected_download_len,
        };
        self.script
            .run(op, || self.inner.finalize(expected_download_len))
    }
}

/// Tests of the script itself, against real temporary files.
#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;

    use super::{Action, Event, Match, Op, ScriptedSinkFactory, WriteScript};
    use crate::memory::SegmentedBytes;
    use crate::operation::download::body::{new_recv_body_with_disk_mode, BodyWriter, ChunkOutput};
    use crate::operation::download::recv_buffer::DrainMode;
    use crate::operation::download::sink::SinkFactory;
    use crate::operation::download::test_util::fixtures::{
        managed_test_handle, object_bytes, object_client,
    };
    use crate::operation::download::{Download, DownloadInput};

    /// How long a test waits for a scripted hold to start.
    const HOLD_TIMEOUT: Duration = Duration::from_secs(5);

    /// A disk body writer over `file` through a sink created from `script`.
    fn scripted_writer(
        script: &Arc<WriteScript>,
        file: std::fs::File,
        expected_len: u64,
    ) -> BodyWriter {
        let sink = ScriptedSinkFactory(Arc::clone(script)).create(file, false);
        let (writer, _consumer) = new_recv_body_with_disk_mode(sink);
        writer.prepare(0, expected_len).unwrap();
        writer
    }

    /// Claims the next slot of `writer` and fills it with `data` at object
    /// offset `offset`.
    fn fill(writer: &BodyWriter, offset: u64, data: &'static [u8]) {
        let slot = writer.claim();
        let seq = slot.seq();
        slot.fill(ChunkOutput {
            seq,
            offset,
            data: SegmentedBytes::from(Bytes::from_static(data)),
            metadata: Default::default(),
        });
    }

    /// A held write blocks its thread before reaching the file and lands only
    /// once released.
    #[test]
    fn held_write_blocks_until_released() {
        let script = Arc::new(WriteScript::default()).on(Match::NthWrite(1), Action::Hold);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let writer = scripted_writer(&script, std::fs::File::create(&path).unwrap(), 4);
        fill(&writer, 0, b"abcd");

        std::thread::scope(|scope| {
            let drain = scope.spawn(|| writer.drain(DrainMode::Eager));
            let held = script.wait_held(HOLD_TIMEOUT);
            assert_eq!(held.op(), Op::Write { pos: 0, len: 4 });
            assert!(
                std::fs::read(&path).unwrap().is_empty(),
                "a held write must not reach the file"
            );
            script.mark("held");
            held.release();
            drain.join().unwrap().unwrap();
        });

        assert_eq!(std::fs::read(&path).unwrap(), b"abcd");
        let write = Op::Write { pos: 0, len: 4 };
        assert_eq!(
            script.events(),
            [
                Event::Started(write),
                Event::Mark("held"),
                Event::Finished(write, Ok(())),
            ]
        );
    }

    /// A held write released with an error fails without touching the file.
    #[test]
    fn held_write_released_with_error_fails() {
        let script = Arc::new(WriteScript::default()).on(Match::NthWrite(1), Action::Hold);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let writer = scripted_writer(&script, std::fs::File::create(&path).unwrap(), 4);
        fill(&writer, 0, b"abcd");

        let error = std::thread::scope(|scope| {
            let drain = scope.spawn(|| writer.drain(DrainMode::Eager));
            script
                .wait_held(HOLD_TIMEOUT)
                .release_err(io::ErrorKind::StorageFull);
            drain.join().unwrap().expect_err("released with an error")
        });

        assert_eq!(error.source.kind(), io::ErrorKind::StorageFull);
        assert!(std::fs::read(&path).unwrap().is_empty());
    }

    /// A fail rule matches the first write at or after its offset, and only
    /// that write.
    #[test]
    fn fail_rule_matches_first_write_at_or_after_offset() {
        let script = Arc::new(WriteScript::default()).on(
            Match::WriteAtOrAfter(2),
            Action::Fail(io::ErrorKind::StorageFull),
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let writer = scripted_writer(&script, std::fs::File::create(&path).unwrap(), 6);

        fill(&writer, 0, b"ab");
        writer.drain(DrainMode::Eager).unwrap();
        fill(&writer, 2, b"cd");
        let error = writer
            .drain(DrainMode::Eager)
            .expect_err("the write at offset 2 is scripted to fail");
        assert_eq!(error.source.kind(), io::ErrorKind::StorageFull);
        fill(&writer, 4, b"ef");
        writer.drain(DrainMode::Eager).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[..2], b"ab");
        assert_eq!(&contents[2..4], [0, 0], "the failed write must not land");
        assert_eq!(&contents[4..], b"ef");
        let failed = Op::Write { pos: 2, len: 2 };
        assert!(script
            .events()
            .contains(&Event::Finished(failed, Err(io::ErrorKind::StorageFull))));
    }

    /// A finalize rule fails the resize and leaves the destination length
    /// unchanged.
    #[test]
    fn finalize_rule_fails_the_resize() {
        let script = Arc::new(WriteScript::default()).on(
            Match::Finalize,
            Action::Fail(io::ErrorKind::PermissionDenied),
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        std::fs::write(&path, b"stale contents").unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let writer = scripted_writer(&script, file, 3);
        fill(&writer, 0, b"new");

        let error = writer
            .finalize(3)
            .expect_err("finalize is scripted to fail");
        assert_eq!(error.source.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(std::fs::read(&path).unwrap(), b"newle contents");
        assert_eq!(
            script.events().last(),
            Some(&Event::Finished(
                Op::Finalize { len: 3 },
                Err(io::ErrorKind::PermissionDenied)
            ))
        );
    }

    /// Every destination operation of a path download goes through the sink
    /// factory it was orchestrated with.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn path_download_writes_through_its_sink_factory() {
        const MIB: usize = 1024 * 1024;
        let object = object_bytes(5 * MIB + 1024);
        let script = Arc::new(WriteScript::default());
        let config = crate::Config::builder()
            .client(object_client(Arc::clone(&object)))
            .part_size(crate::types::PartSize::Target(5 * MIB as u64))
            .build();
        let handle = managed_test_handle(config, 128);
        let input = DownloadInput::builder()
            .bucket("bucket")
            .key("key")
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("object");

        Download::orchestrate_to_path(
            Arc::clone(&handle),
            input,
            dest.clone(),
            None,
            None,
            &ScriptedSinkFactory(Arc::clone(&script)),
        )
        .await
        .unwrap()
        .join()
        .await
        .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), &object[..]);
        let events = script.events();
        let written: usize = events
            .iter()
            .map(|event| match event {
                Event::Finished(Op::Write { len, .. }, Ok(())) => *len,
                _ => 0,
            })
            .sum();
        assert_eq!(written, object.len(), "every byte went through the script");
        assert_eq!(
            events.last(),
            Some(&Event::Finished(
                Op::Finalize {
                    len: object.len() as u64
                },
                Ok(())
            ))
        );
    }

    /// Markers and operations are logged in the order they happen, and a
    /// delayed write still lands.
    #[test]
    fn markers_order_against_operations() {
        let script = Arc::new(WriteScript::default())
            .on(Match::NthWrite(2), Action::Delay(Duration::from_millis(10)));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let writer = scripted_writer(&script, std::fs::File::create(&path).unwrap(), 4);

        script.mark("start");
        fill(&writer, 0, b"ab");
        writer.drain(DrainMode::Eager).unwrap();
        script.mark("between");
        fill(&writer, 2, b"cd");
        writer.finalize(4).unwrap();
        script.mark("end");

        let first = Op::Write { pos: 0, len: 2 };
        let second = Op::Write { pos: 2, len: 2 };
        let finalize = Op::Finalize { len: 4 };
        assert_eq!(
            script.events(),
            [
                Event::Mark("start"),
                Event::Started(first),
                Event::Finished(first, Ok(())),
                Event::Mark("between"),
                Event::Started(second),
                Event::Finished(second, Ok(())),
                Event::Started(finalize),
                Event::Finished(finalize, Ok(())),
                Event::Mark("end"),
            ]
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"abcd");
    }
}
