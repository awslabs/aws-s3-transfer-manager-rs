//! RUST-1224 shape spike: entry-scoped events, verified against the exact
//! `aws s3` output lines. Compiled standalone with a stub `Error`.
#![deny(missing_docs)]
#![allow(dead_code)]

use std::fmt;
use std::path::Path;
use std::sync::Arc;

// ---------------------------------------------------------------- stub of crate::error::Error
/// Stand-in for `crate::error::Error`.
#[derive(Debug)]
pub struct Error(String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ================================================================ the shape

/// Which way bytes move for one entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Direction {
    /// Local filesystem to S3.
    Upload,
    /// S3 to local filesystem.
    Download,
    /// S3 to S3.
    Copy,
}

/// One end of an entry: an S3 object or a local path.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Endpoint {
    /// An S3 object. `bucket` is interned once per operation.
    #[non_exhaustive]
    S3 {
        /// Bucket, access point ARN, or outpost ARN, as the caller gave it.
        bucket: Arc<str>,
        /// Full object key, not the relative key.
        key: Arc<str>,
    },
    /// A local file. Stored as constructed from the caller's root, never
    /// canonicalized, so a relative root prints back as the caller wrote it.
    #[non_exhaustive]
    Local {
        /// Path to the file.
        path: Arc<Path>,
    },
}

impl fmt::Display for Endpoint {
    /// Renders `s3://bucket/key` or the path, lossily for non-UTF-8 paths.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::S3 { bucket, key } => write!(f, "s3://{bucket}/{key}"),
            Endpoint::Local { path } => write!(f, "{}", path.display()),
        }
    }
}

/// An entry's name relative to its own root, `/`-separated. The join key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelativeKey(Arc<[u8]>);

impl RelativeKey {
    /// The key as bytes. Bytes because a local filename need not be UTF-8.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    /// The key as text, or `None` if it is not valid UTF-8.
    pub fn to_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl fmt::Display for RelativeKey {
    /// Lossy. Use [`RelativeKey::as_bytes`] to compare or group.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
}

/// What an event is about: one entry, and where its two ends are.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Entry {
    direction: Direction,
    relative_key: Option<RelativeKey>,
    source: Endpoint,
    destination: Endpoint,
}

impl Entry {
    /// Local to S3. One of three constructors, so `Local -> Local` is unrepresentable.
    pub(crate) fn upload(
        relative_key: Option<RelativeKey>,
        source: Arc<Path>,
        bucket: Arc<str>,
        key: Arc<str>,
    ) -> Self {
        Self {
            direction: Direction::Upload,
            relative_key,
            source: Endpoint::Local { path: source },
            destination: Endpoint::S3 { bucket, key },
        }
    }

    /// S3 to local.
    pub(crate) fn download(
        relative_key: Option<RelativeKey>,
        bucket: Arc<str>,
        key: Arc<str>,
        destination: Arc<Path>,
    ) -> Self {
        Self {
            direction: Direction::Download,
            relative_key,
            source: Endpoint::S3 { bucket, key },
            destination: Endpoint::Local { path: destination },
        }
    }

    /// S3 to S3.
    pub(crate) fn copy(
        relative_key: Option<RelativeKey>,
        source: (Arc<str>, Arc<str>),
        destination: (Arc<str>, Arc<str>),
    ) -> Self {
        Self {
            direction: Direction::Copy,
            relative_key,
            source: Endpoint::S3 {
                bucket: source.0,
                key: source.1,
            },
            destination: Endpoint::S3 {
                bucket: destination.0,
                key: destination.1,
            },
        }
    }

    /// Which way bytes move. Total: set by construction, never derived.
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// The join key between the two roots, `/`-separated.
    ///
    /// `None` when this transfer is not an entry under a pair of roots — a
    /// single-object `upload()`/`download()`, where the caller named both ends
    /// itself and no root exists to be relative to.
    pub fn relative_key(&self) -> Option<&RelativeKey> {
        self.relative_key.as_ref()
    }

    /// Where the entry is read from. Named even when nothing is present there:
    /// that absence is what licenses a `Delete`.
    pub fn source(&self) -> &Endpoint {
        &self.source
    }

    /// Where the entry is written to, and the target of a `Delete`.
    pub fn destination(&self) -> &Endpoint {
        &self.destination
    }
}

/// Why an entry needs transferring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransferReason {
    /// Not present at the destination.
    NotAtDestination,
    /// Present at both ends, sizes differ.
    SizeDiffers,
    /// Present at both ends, times differ.
    TimeDiffers,
    /// Comparison was bypassed by the caller.
    Forced,
}

/// Why an entry was left alone.
///
/// Severity is deliberately absent: a case conflict is a warning, a skip, or a
/// failure depending on the caller's setting, so it cannot be structural.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SkipReason {
    /// Present at both ends and they matched.
    Unchanged,
    /// Removed by an include/exclude filter.
    ExcludedByFilter,
    /// In an archival storage class and not restored.
    ArchivalStorageClass,
    /// Two keys differing only in case land on one file.
    CaseConflict,
    /// Not a regular file: device, FIFO, socket, or an unfollowed symlink.
    /// Present, so it holds a delete back.
    Untransferable,
    /// Present only at the destination, with delete mode off.
    DestinationOnly,
    /// The source side could not be seen, so nothing may be deleted here.
    #[non_exhaustive]
    SourceUnknown {
        /// The enumeration failure covering this key. `Arc` because one
        /// unreadable directory makes every key under it unknown.
        cause: Arc<Error>,
    },
    /// The destination side could not be seen, so nothing may be written here.
    #[non_exhaustive]
    DestinationUnknown {
        /// The enumeration failure covering this key.
        cause: Arc<Error>,
    },
}

/// The action decided for an entry, and the reason for it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Decision {
    /// Move the entry. Direction comes from [`Entry::direction`].
    #[non_exhaustive]
    Transfer {
        /// Why it needs moving.
        reason: TransferReason,
    },
    /// Remove [`Entry::destination`]. The reason is the action: present at the
    /// destination, absent at the source, with delete mode on.
    #[non_exhaustive]
    Delete {},
    /// Do nothing.
    #[non_exhaustive]
    Skip {
        /// Why nothing was done.
        reason: SkipReason,
    },
}

/// How an attempted action ended.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Outcome {
    /// The action completed. No byte count: this stream is lossy, so a summed
    /// total would silently disagree with the operation's own result.
    #[non_exhaustive]
    Succeeded {},
    /// The action failed.
    #[non_exhaustive]
    Failed {
        /// What went wrong.
        error: Arc<Error>,
    },
    /// The action was cancelled, including by a sibling's failure.
    #[non_exhaustive]
    Cancelled {},
}

/// One fact about one entry.
///
/// Every event stands alone: delivery is lossy, so nothing here depends on
/// having seen an earlier event.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EntryEvent {
    /// A decision was reached for this entry. Exactly one per entry, in every
    /// run, dry or not.
    #[non_exhaustive]
    Decided {
        /// The entry.
        entry: Entry,
        /// What was decided and why.
        decision: Decision,
    },
    /// An attempted action reached a terminal state. Fires if and only if the
    /// action was attempted — so never for a [`Decision::Skip`], and never in a
    /// dry run.
    #[non_exhaustive]
    Settled {
        /// The entry. Repeated so no consumer needs a side map.
        entry: Entry,
        /// Repeated for the same reason: a lost `Decided` must not cost the
        /// caller the verb.
        decision: Decision,
        /// How it ended.
        outcome: Outcome,
    },
}

// ================================================================ consumer 1: the CLI printer

/// `results.py` → `ResultPrinter.SRC_DEST_LOCATION_FORMAT` / `_get_transfer_location`.
fn location(entry: &Entry, decision: &Decision) -> String {
    match decision {
        // A delete has one location. `dest is None` in the CLI's own result.
        Decision::Delete {} => format!("{}", entry.destination()),
        _ => format!("{} to {}", entry.source(), entry.destination()),
    }
}

fn verb(entry: &Entry, decision: &Decision) -> &'static str {
    match decision {
        Decision::Delete {} => "delete",
        _ => match entry.direction() {
            Direction::Upload => "upload",
            Direction::Download => "download",
            Direction::Copy => "copy",
        },
    }
}

/// The warning text `aws s3 sync` prints for a skip, or `None` for a silent one.
fn warning(entry: &Entry, reason: &SkipReason) -> Option<String> {
    match reason {
        // Silent in the CLI: a matched pair and a filtered-out key are the
        // expected case, and a destination-only key with --delete off is a no-op.
        SkipReason::Unchanged | SkipReason::ExcludedByFilter | SkipReason::DestinationOnly => None,
        SkipReason::Untransferable => Some(format!(
            "warning: Skipping file {}. File is character special device, \
             block special device, FIFO, or socket.",
            entry.source()
        )),
        SkipReason::ArchivalStorageClass => Some(format!(
            "warning: Skipping file {}. Object is of storage class GLACIER. \
             Unable to perform download operations on GLACIER objects.",
            entry.source()
        )),
        SkipReason::CaseConflict => Some(format!(
            "warning: {} conflicts with another key that differs only in case.",
            entry.destination()
        )),
        SkipReason::SourceUnknown { cause } => Some(format!(
            "warning: could not read the source for {}, holding back its delete: {cause}",
            entry.destination()
        )),
        SkipReason::DestinationUnknown { cause } => Some(format!(
            "warning: could not read the destination for {}, not transferring: {cause}",
            entry.source()
        )),
    }
}

/// Consumer 1: `aws s3 sync` / `cp` / `mv`, printing per entry while the run goes.
pub fn cli_line(event: &EntryEvent) -> Option<String> {
    match event {
        // A skip is settled the moment it is decided: nothing is attempted, so
        // no `Settled` follows and this is the only chance to report it.
        EntryEvent::Decided {
            entry,
            decision: Decision::Skip { reason },
        } => warning(entry, reason),

        // A transfer and a delete print on their terminal, which is where the
        // CLI prints them: `SuccessResult` comes from a completed future.
        EntryEvent::Decided { .. } => None,

        EntryEvent::Settled {
            entry,
            decision,
            outcome,
        } => match outcome {
            Outcome::Succeeded {} => Some(format!(
                "{}: {}",
                verb(entry, decision),
                location(entry, decision)
            )),
            Outcome::Failed { error } => Some(format!(
                "{} failed: {} {}",
                verb(entry, decision),
                location(entry, decision),
                error
            )),
            // One ctrl-C is one command-level message, not N per-entry failures.
            Outcome::Cancelled {} => None,
        },
    }
}

// ================================================================ consumer 2: --dryrun

/// Consumer 2: `aws s3 sync --dryrun`, where the events are the output.
///
/// No `(dryrun)` discriminator on the event: the caller asked for a dry run, so
/// every event it receives is one.
pub fn dryrun_line(event: &EntryEvent) -> Option<String> {
    match event {
        EntryEvent::Decided { entry, decision } => match decision {
            Decision::Transfer { .. } | Decision::Delete {} => Some(format!(
                "(dryrun) {}: {}",
                verb(entry, decision),
                location(entry, decision)
            )),
            Decision::Skip { reason } => warning(entry, reason),
        },
        // Nothing is attempted in a dry run, so this arm cannot be reached there.
        EntryEvent::Settled { .. } => None,
    }
}

// ================================================================ the assertions

fn rk(s: &str) -> Option<RelativeKey> {
    Some(RelativeKey(Arc::from(s.as_bytes())))
}

fn main() {
    let mut checked = 0;
    let mut check = |got: Option<String>, want: Option<&str>| {
        assert_eq!(got.as_deref(), want, "line mismatch");
        checked += 1;
        if let Some(l) = got {
            println!("{l}");
        }
    };

    // 1. upload, succeeded. FR-Root-3's worked example: /data -> s3://bucket/backup.
    let up = Entry::upload(
        rk("logs/app.log"),
        Arc::from(Path::new("/data/logs/app.log")),
        Arc::from("bucket"),
        Arc::from("backup/logs/app.log"),
    );
    check(
        cli_line(&EntryEvent::Settled {
            entry: up.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::NotAtDestination,
            },
            outcome: Outcome::Succeeded {},
        }),
        Some("upload: /data/logs/app.log to s3://bucket/backup/logs/app.log"),
    );

    // 1b. the prompt's own line, with a relative root printed back verbatim.
    let up_rel = Entry::upload(
        rk("a"),
        Arc::from(Path::new("./a")),
        Arc::from("b"),
        Arc::from("a"),
    );
    check(
        cli_line(&EntryEvent::Settled {
            entry: up_rel.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::SizeDiffers,
            },
            outcome: Outcome::Succeeded {},
        }),
        Some("upload: ./a to s3://b/a"),
    );

    // 2. download.
    let down = Entry::download(
        rk("a"),
        Arc::from("b"),
        Arc::from("a"),
        Arc::from(Path::new("./a")),
    );
    check(
        cli_line(&EntryEvent::Settled {
            entry: down.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::TimeDiffers,
            },
            outcome: Outcome::Succeeded {},
        }),
        Some("download: s3://b/a to ./a"),
    );

    // 3. copy, cross-bucket.
    let cp = Entry::copy(
        rk("a"),
        (Arc::from("b1"), Arc::from("a")),
        (Arc::from("b2"), Arc::from("a")),
    );
    check(
        cli_line(&EntryEvent::Settled {
            entry: cp,
            decision: Decision::Transfer {
                reason: TransferReason::Forced,
            },
            outcome: Outcome::Succeeded {},
        }),
        Some("copy: s3://b1/a to s3://b2/a"),
    );

    // 4. delete: one location, the destination.
    let del = Entry::upload(
        rk("gone.txt"),
        Arc::from(Path::new("/data/gone.txt")),
        Arc::from("b"),
        Arc::from("backup/gone.txt"),
    );
    check(
        cli_line(&EntryEvent::Settled {
            entry: del.clone(),
            decision: Decision::Delete {},
            outcome: Outcome::Succeeded {},
        }),
        Some("delete: s3://b/backup/gone.txt"),
    );

    // 5. failure: `{verb} failed: {location} {exception}`.
    check(
        cli_line(&EntryEvent::Settled {
            entry: up_rel.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::NotAtDestination,
            },
            outcome: Outcome::Failed {
                error: Arc::new(Error(
                    "An error occurred (AccessDenied) when calling the PutObject operation: \
                     Access Denied"
                        .into(),
                )),
            },
        }),
        Some(
            "upload failed: ./a to s3://b/a An error occurred (AccessDenied) when calling \
             the PutObject operation: Access Denied",
        ),
    );

    // 6. a delete that failed. Same shape, no extra variant.
    check(
        cli_line(&EntryEvent::Settled {
            entry: del.clone(),
            decision: Decision::Delete {},
            outcome: Outcome::Failed {
                error: Arc::new(Error("AccessDenied".into())),
            },
        }),
        Some("delete failed: s3://b/backup/gone.txt AccessDenied"),
    );

    // 7. an unchanged entry prints nothing, and produces exactly one event.
    check(
        cli_line(&EntryEvent::Decided {
            entry: up_rel.clone(),
            decision: Decision::Skip {
                reason: SkipReason::Unchanged,
            },
        }),
        None,
    );

    // 8. skipped-with-warning: present, so the delete is held back.
    let fifo = Entry::upload(
        rk("pipe"),
        Arc::from(Path::new("/data/pipe")),
        Arc::from("b"),
        Arc::from("backup/pipe"),
    );
    check(
        cli_line(&EntryEvent::Decided {
            entry: fifo,
            decision: Decision::Skip {
                reason: SkipReason::Untransferable,
            },
        }),
        Some(
            "warning: Skipping file /data/pipe. File is character special device, \
             block special device, FIFO, or socket.",
        ),
    );

    // 9. skipped-unknown, distinguishable from skipped-unchanged and naming the side.
    check(
        cli_line(&EntryEvent::Decided {
            entry: del.clone(),
            decision: Decision::Skip {
                reason: SkipReason::SourceUnknown {
                    cause: Arc::new(Error("failed to read /data: permission denied".into())),
                },
            },
        }),
        Some(
            "warning: could not read the source for s3://b/backup/gone.txt, holding back \
             its delete: failed to read /data: permission denied",
        ),
    );

    // 10. dry run: the same lines, one event per entry, no terminal.
    check(
        dryrun_line(&EntryEvent::Decided {
            entry: up_rel.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::NotAtDestination,
            },
        }),
        Some("(dryrun) upload: ./a to s3://b/a"),
    );
    check(
        dryrun_line(&EntryEvent::Decided {
            entry: del.clone(),
            decision: Decision::Delete {},
        }),
        Some("(dryrun) delete: s3://b/backup/gone.txt"),
    );
    check(
        dryrun_line(&EntryEvent::Decided {
            entry: down.clone(),
            decision: Decision::Transfer {
                reason: TransferReason::SizeDiffers,
            },
        }),
        Some("(dryrun) download: s3://b/a to ./a"),
    );

    // 11. consumer 3, mv --recursive: acts on the terminal, and the root of a
    // directory operation is not an entry, so there is no event that names the
    // whole tree for it to unlink.
    let ev = EntryEvent::Settled {
        entry: up.clone(),
        decision: Decision::Transfer {
            reason: TransferReason::NotAtDestination,
        },
        outcome: Outcome::Succeeded {},
    };
    if let EntryEvent::Settled {
        entry,
        decision: Decision::Transfer { .. },
        outcome: Outcome::Succeeded {},
    } = &ev
    {
        if let Endpoint::Local { path } = entry.source() {
            assert_eq!(path.as_ref(), Path::new("/data/logs/app.log"));
            checked += 1;
        }
    }

    // 12. consumer 5, grouping failures by cause and common prefix: the join key
    // is bytes and ordered, so a prefix scan needs no re-derivation.
    let keys: Vec<RelativeKey> = ["logs/2019/a", "logs/2019/b", "logs/2020/a"]
        .iter()
        .map(|s| RelativeKey(Arc::from(s.as_bytes())))
        .collect();
    assert!(keys[0] < keys[2] && keys[0].as_bytes().starts_with(b"logs/2019/"));
    checked += 1;

    println!("\n{checked} assertions passed");
}
