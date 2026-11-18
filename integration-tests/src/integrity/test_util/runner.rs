/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`Case::run`]: connect, upload, install the mock's faults, download to the sink and check
//! the outcome.

use std::time::Duration;

use aws_sdk_s3_transfer_manager::error::Error;
use aws_sdk_s3_transfer_manager::operation::download::DownloadOutput;
use s3_mock_server::{FaultTarget, FaultType, Occurrence};

use super::vocab::{Action, Case, CaseTarget, Hold, Sink};
use crate::assertions::is_download_temp_file;
use crate::harness::{ClientOptions, TmTestClient};

/// How long a held body waits for the other ranges before it is released anyway. The case then
/// fails its hold check rather than hanging.
pub(crate) const HOLD_DEADLINE: Duration = Duration::from_secs(30);

/// Bytes a truncated GET sends before it is cut.
///
/// Every range a table truncates is longer, so each truncation cuts its GET short; a range this
/// short or shorter would be sent in full and fail the truncate check.
const TRUNCATE_AFTER: u64 = 1024;

/// Contents written to a path sink's destination before the download, so a failed download can
/// be shown to leave it unchanged.
pub(crate) const SENTINEL: &[u8] = b"sentinel: the download must not replace this file";

/// What a download delivered.
pub(crate) struct Delivered {
    /// The bytes the sink received: the stream's items, or the destination file's contents.
    pub(crate) bytes: Vec<u8>,
    /// The download's result.
    pub(crate) result: Result<DownloadOutput, Error>,
    /// For the stream sink, whether `Body::next` ended with an error.
    pub(crate) stream_ended_in_error: bool,
    /// For the path sink, whether a `.s3tmp.` file remained in the destination's directory.
    pub(crate) temp_file_remains: bool,
}

/// What one execution of a case produced, for [`Case::check`].
pub(crate) struct Execution {
    pub(crate) delivered: Delivered,
    /// On a mock target, every request the mock recorded during the download; `None` on real S3.
    pub(crate) requests: Option<Vec<s3_mock_server::RequestRecord>>,
    /// The object's full key, as the requests name it.
    pub(crate) key: String,
}

/// The result of a check: `Err` holds the message the case panics with.
pub(crate) type Check = Result<(), String>;

impl Case {
    /// Runs the case on `target` and panics, naming the case and printing it, if any check fails
    /// or any step panics.
    pub(crate) async fn run(&'static self, target: CaseTarget) {
        let message = match tokio::spawn(self.try_run(target)).await {
            Ok(Ok(())) => return,
            Ok(Err(message)) => message,
            Err(e) => match e.try_into_panic() {
                Ok(panic) => panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "panicked".to_owned()),
                Err(e) => format!("{e}"),
            },
        };
        panic!("{}: {message}\n{self:#?}", self.path);
    }

    async fn try_run(&'static self, target: CaseTarget) -> Check {
        let execution = self.execute(target).await;
        self.check(&execution)
    }

    /// Uploads the case's object to a fresh connection to `target`, installs its fault and hold
    /// on a mock target, downloads to its sink, and returns what was delivered and, on a mock
    /// target, the requests the mock recorded for the object.
    ///
    /// Panics if the upload or its checks fail.
    pub(crate) async fn execute(&self, target: CaseTarget) -> Execution {
        let t = target.harness().connect().await;
        let key = self.key();
        self.object.upload(&t, &key, self.seed()).await;
        if target.is_mock() {
            self.install_faults(&t, &key);
        }
        let client = t.client_with(ClientOptions {
            part_size: self.download.part_size,
            concurrency: self.download.concurrency,
            response_checksums_when_required: !self.download.validation,
            ..Default::default()
        });
        let delivered = self.download(&client, &t, &key).await;
        let requests = t.mock().map(|mock| mock.requests());
        let execution = Execution {
            delivered,
            requests,
            key: t.key(&key),
        };
        t.shutdown().await;
        execution
    }

    /// Checks `execution` against the case: the outcome on every target, and on a mock target,
    /// the request journal. Returns the first failed check's message.
    pub(crate) fn check(&self, execution: &Execution) -> Check {
        self.check_outcome(&execution.delivered)?;
        match &execution.requests {
            Some(requests) => self.check_journal(requests, &execution.key),
            None => Ok(()),
        }
    }

    /// The object's key: the case's path, so no two cases share an object.
    fn key(&self) -> String {
        self.path.replace("::", "/")
    }

    /// The object's seed: the case's path hashed with FNV-1a and folded to 32 bits. Distinct
    /// across the tables' cases, which `table_cases_are_well_formed` checks.
    pub(crate) fn seed(&self) -> u32 {
        // FNV-1a.
        let hash = self.path.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
        (hash ^ (hash >> 32)) as u32
    }

    /// The number of [`range_size`](Case::range_size) ranges covering the whole object, at least
    /// 1. Ignores a caller range, and the automatic part size's realignment to stored parts.
    pub(crate) fn ranges(&self) -> u64 {
        self.object.len.div_ceil(self.range_size()).max(1)
    }

    fn install_faults(&self, t: &TmTestClient, key: &str) {
        let mock = t.mock().expect("a mock target");
        let covering = |offset| FaultTarget::object(t.bucket(), t.key(key)).covering(offset);
        if let Some(fault) = self.fault {
            let offset = self.byte(fault.at);
            let corrupt = FaultType::CorruptByte { offset };
            let truncate = FaultType::TruncateBody {
                after_bytes: TRUNCATE_AFTER,
            };
            match fault.action {
                Action::Corrupt => {
                    mock.insert_fault_for(covering(offset), corrupt, 0, Occurrence::Always)
                }
                Action::Truncate => {
                    mock.insert_fault_for(covering(offset), truncate, 0, Occurrence::Once)
                }
                Action::CorruptRetry => {
                    mock.insert_fault_for(covering(offset), truncate, 0, Occurrence::Once);
                    mock.insert_fault_for(covering(offset), corrupt, 0, Occurrence::Once);
                }
            }
        }
        if let Some(Hold::FirstRange) = self.hold {
            let others = u32::try_from(self.ranges() - 1).expect("range count");
            mock.insert_fault_for(
                covering(0),
                FaultType::HoldBody {
                    others,
                    deadline: HOLD_DEADLINE,
                },
                0,
                Occurrence::Once,
            );
        }
    }

    async fn download(
        &self,
        client: &aws_sdk_s3_transfer_manager::Client,
        t: &TmTestClient,
        key: &str,
    ) -> Delivered {
        let mut builder = client.download().bucket(t.bucket()).key(t.key(key));
        if let Some((first, last)) = self.download.range {
            builder = builder.range(format!("bytes={first}-{last}"));
        }
        match self.sink {
            Sink::Stream => {
                let mut handle = builder.initiate().expect("initiate download");
                let mut bytes = Vec::new();
                let mut stream_ended_in_error = false;
                while let Some(item) = handle.body_mut().next().await {
                    match item {
                        Ok(chunk) => bytes.extend_from_slice(&chunk.data.into_contiguous()),
                        Err(_) => {
                            stream_ended_in_error = true;
                            break;
                        }
                    }
                }
                Delivered {
                    bytes,
                    result: handle.join().await,
                    stream_ended_in_error,
                    temp_file_remains: false,
                }
            }
            Sink::Path => {
                let dir = tempfile::tempdir().expect("create destination directory");
                let dest = dir.path().join("object");
                std::fs::write(&dest, SENTINEL).expect("write sentinel");
                let result = match builder.write_to_path(&dest).await {
                    Ok(handle) => handle.join().await,
                    Err(e) => Err(e),
                };
                let temp_file_remains = std::fs::read_dir(dir.path())
                    .expect("list destination directory")
                    .any(|entry| is_download_temp_file(&entry.expect("directory entry").path()));
                Delivered {
                    bytes: std::fs::read(&dest).expect("read destination"),
                    result,
                    stream_ended_in_error: false,
                    temp_file_remains,
                }
            }
            Sink::File => {
                let file = tempfile::tempfile().expect("create destination file");
                let reader = file.try_clone().expect("clone destination file");
                let result = match builder.write_to_file(file) {
                    Ok(handle) => handle.join().await,
                    Err(e) => Err(e),
                };
                Delivered {
                    bytes: read_all(reader),
                    result,
                    stream_ended_in_error: false,
                    temp_file_remains: false,
                }
            }
        }
    }
}

fn read_all(mut file: std::fs::File) -> Vec<u8> {
    use std::io::{Read, Seek};
    file.rewind().expect("rewind destination file");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).expect("read destination file");
    bytes
}
