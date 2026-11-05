/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Retention scenarios, destination probes, and managed-runtime teardown for download tests.

use super::{common, dummy_expected_request, rand_data, simple_object_connector};
use aws_sdk_s3_transfer_manager::{
    metrics::unit::ByteUnit,
    types::{ConcurrencyMode, PartSize},
    RuntimeConfig,
};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use std::time::Duration;

/// Observes whether another descriptor in this process keeps a file open.
///
/// Unix pins the file's identity and compares it across open descriptors,
/// excluding its own descriptor. Renaming or unlinking preserves the identity.
/// Windows attempts an exclusive open, so the path must exist when probed.
pub(super) struct OpenFileProbe {
    #[cfg(unix)]
    id: (u64, u64),
    #[cfg(unix)]
    pin: std::fs::File,
    #[cfg(windows)]
    path: std::path::PathBuf,
}

impl OpenFileProbe {
    /// On Unix `path` must exist now; on Windows it must exist when probed.
    pub(super) fn new(path: &std::path::Path) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let pin = std::fs::File::open(path).unwrap();
            let meta = pin.metadata().unwrap();
            Self {
                id: (meta.dev(), meta.ino()),
                pin,
            }
        }
        #[cfg(windows)]
        {
            Self {
                path: path.to_owned(),
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn is_open(&self) -> bool {
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        let pin_path = std::path::PathBuf::from(format!("/proc/self/fd/{}", self.pin.as_raw_fd()));
        // Path lookup observes each descriptor through procfs while its owner
        // may close it. Paths that disappear before lookup are skipped.
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path() != pin_path)
            .any(|entry| {
                std::fs::metadata(entry.path())
                    .is_ok_and(|meta| (meta.dev(), meta.ino()) == self.id)
            })
    }

    // `st_dev` and `st_ino` widths differ across Unix targets.
    #[cfg(all(unix, not(target_os = "linux")))]
    #[allow(clippy::unnecessary_cast)]
    fn is_open(&self) -> bool {
        use std::os::fd::AsRawFd;
        let pin = self.pin.as_raw_fd();
        let fds: Vec<i32> = std::fs::read_dir("/dev/fd")
            .unwrap()
            .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
            .filter(|&fd| fd != pin)
            .collect();
        fds.into_iter().any(|fd| {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: `fstat` only writes into `stat`, which is read only after
            // a successful return. A descriptor closed since listing fails
            // with EBADF and is skipped.
            unsafe {
                libc::fstat(fd, stat.as_mut_ptr()) == 0 && {
                    let stat = stat.assume_init();
                    (stat.st_dev as u64, stat.st_ino as u64) == self.id
                }
            }
        })
    }

    #[cfg(windows)]
    fn is_open(&self) -> bool {
        use std::os::windows::fs::OpenOptionsExt;
        const ERROR_SHARING_VIOLATION: i32 = 32;
        match std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&self.path)
        {
            Ok(_) => false,
            Err(e) if e.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => true,
            Err(e) => panic!("cannot probe {}: {e}", self.path.display()),
        }
    }

    /// Waits up to one second for every other descriptor on this file to close.
    ///
    /// The deadline bounds the test, not destination release by the public API.
    pub(super) async fn assert_released(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.is_open() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the transfer manager retained the destination file");
    }
}

/// Drops the client and waits for its managed threads to join.
pub(super) async fn drop_and_await_shutdown(tm: aws_sdk_s3_transfer_manager::Client) {
    let completion = tm.shutdown_completion();
    drop(tm);
    await_shutdown(completion).await;
}

/// Waits for managed-runtime teardown, if the client owns a managed runtime.
pub(super) async fn await_shutdown(
    completion: Option<aws_sdk_s3_transfer_manager::ShutdownCompletion>,
) {
    if let Some(completion) = completion {
        tokio::time::timeout(Duration::from_secs(5), completion.wait())
            .await
            .expect("the client's runtime did not shut down")
            .expect("managed runtime shutdown failed");
    }
}

/// The destination a single-object download writes through.
#[derive(Clone, Copy, Debug)]
pub(super) enum Destination {
    /// `write_to_path`: a temporary file beside the path, renamed on success.
    Path,
    /// `write_to_file`: a file the caller opened and moved in.
    File,
}

/// How a single-object download ends.
#[derive(Clone, Copy, Debug)]
pub(super) enum Ending {
    /// `join` succeeds.
    Success,
    /// A part request fails after discovery, so `join` fails.
    BodyFailure,
    /// `abort` returns.
    Abort,
    /// The handle is dropped without `join`.
    DropHandle,
}

/// An ended download releases its destination through ordinary retirement,
/// while the client is still alive.
///
/// A transient work or polling reference can outlive `join`, `abort`, or handle
/// drop. The probe waits for those references to retire without advancing the
/// epoch collector. A retained transfer fails the bounded wait.
pub(super) async fn assert_download_releases_destination(
    destination: Destination,
    ending: Ending,
    runtime_config: RuntimeConfig,
) {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let data = rand_data(2 * part_size);
    let http_client = match ending {
        // Discovery returns the first part; the second part request fails.
        Ending::BodyFailure => StaticReplayClient::new(vec![
            ReplayEvent::new(
                dummy_expected_request(),
                http::Response::builder()
                    .status(200)
                    .header("Content-Length", format!("{part_size}"))
                    .header(
                        "Content-Range",
                        format!("bytes 0-{}/{}", part_size - 1, data.len()),
                    )
                    .header("ETag", "my-etag")
                    .body(SdkBody::from(data.slice(0..part_size)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                dummy_expected_request(),
                http::Response::builder()
                    .status(412)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>PreconditionFailed</Code></Error>"#,
                    ))
                    .unwrap(),
            ),
        ]),
        _ => simple_object_connector(&data, part_size),
    };
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .s3_config(common::s3_config(http_client))
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencyMode::Explicit(1))
        .runtime_config(runtime_config)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");
    let download = tm.download().bucket("test-bucket").key("test-object");
    let handle = match destination {
        Destination::Path => download.write_to_path(&dest_path).await.unwrap(),
        Destination::File => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&dest_path)
                .unwrap();
            download.write_to_file(file).unwrap()
        }
    };
    // The temporary path can be renamed or removed. Unix probes pin its
    // identity; Windows probes keep a hard link for an exclusive open.
    let written = match destination {
        Destination::Path => {
            let temps: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.to_string_lossy().contains(".s3tmp"))
                .collect();
            assert_eq!(temps.len(), 1, "temporary files: {temps:?}");
            #[cfg(unix)]
            {
                temps[0].clone()
            }
            #[cfg(windows)]
            {
                let probe_path = dir.path().join("probe.dat");
                std::fs::hard_link(&temps[0], &probe_path).unwrap();
                probe_path
            }
        }
        _ => dest_path.clone(),
    };
    let probe = OpenFileProbe::new(&written);

    match ending {
        Ending::Success => {
            handle.join().await.unwrap();
            assert_eq!(std::fs::read(&dest_path).unwrap(), data.as_ref());
        }
        Ending::BodyFailure => {
            handle.join().await.unwrap_err();
        }
        Ending::Abort => handle.abort().await,
        Ending::DropHandle => drop(handle),
    }
    probe.assert_released().await;
    drop_and_await_shutdown(tm).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A held file is reported open, and closing it is observed.
    #[test]
    fn open_file_probe_reports_a_held_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held.dat");
        std::fs::write(&path, b"held").unwrap();
        let probe = OpenFileProbe::new(&path);
        assert!(!probe.is_open());

        let held = std::fs::File::open(&path).unwrap();
        assert!(probe.is_open());
        drop(held);
        assert!(!probe.is_open());
    }

    /// A hard link preserves the probe across a rename and unlink of the opened path.
    #[test]
    fn open_file_probe_reports_a_held_file_through_a_hard_link() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held.dat");
        let probe_path = dir.path().join("probe.dat");
        let renamed = dir.path().join("renamed.dat");
        std::fs::write(&path, b"held").unwrap();
        std::fs::hard_link(&path, &probe_path).unwrap();
        let probe = OpenFileProbe::new(&probe_path);
        assert!(!probe.is_open());

        let held = std::fs::File::open(&path).unwrap();
        std::fs::rename(&path, &renamed).unwrap();
        assert!(probe.is_open());
        std::fs::remove_file(&renamed).unwrap();
        assert!(probe.is_open());
        drop(held);
        assert!(!probe.is_open());
    }

    /// An unlinked file whose descriptor is still held is reported open.
    #[cfg(unix)]
    #[test]
    fn open_file_probe_reports_a_held_unlinked_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held.dat");
        std::fs::write(&path, b"held").unwrap();
        let probe = OpenFileProbe::new(&path);
        let held = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert!(probe.is_open());
        drop(held);
        assert!(!probe.is_open());
    }
}
