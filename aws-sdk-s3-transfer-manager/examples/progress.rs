/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Copies one object to or from S3, printing a line for each transfer event and drawing a
//! progress bar from the transfer's view.
//!
//! ```sh
//! cargo run --example progress s3://<bucket>/<key> /local/path
//! cargo run --example progress /local/path s3://<bucket>/<key>
//! ```

use aws_sdk_s3_transfer_manager::events::{
    self, Direction, Outcome, TransferEvent, TransferEventStream, TryRecvError,
};
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::types::{Total, TransferView};
use clap::Parser;
use std::error::Error;
use std::io::Write;
use std::num::NonZeroUsize;
use std::time::Duration;

type BoxError = Box<dyn Error + Send + Sync>;

/// Moves the cursor to the start of the line and erases it, so a redraw replaces the bar.
const CLEAR_LINE: &str = "\r\x1b[2K";

/// Width of the bar, in characters.
const BAR_WIDTH: u64 = 40;

#[derive(Debug, clap::Parser)]
#[command(name = "progress")]
#[command(about = "Copies one object to or from S3 and draws its progress.")]
struct Args {
    /// Source to copy from <S3Uri | Local>
    source: String,

    /// Destination to copy to <S3Uri | Local>
    dest: String,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let config = aws_sdk_s3_transfer_manager::from_env().load().await;
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    // A sink registered on one transfer never drops an event at capacity 2.
    let (sink, stream) = events::channel(NonZeroUsize::new(2).expect("2 is nonzero"));

    match (s3_uri(&args.source), s3_uri(&args.dest)) {
        (Some((bucket, key)), None) => {
            let handle = tm
                .download()
                .bucket(bucket)
                .key(key)
                .events(sink)
                .write_to_path(&args.dest)
                .await?;
            show_progress(stream).await?;
            handle.join().await?;
        }
        (None, Some((bucket, key))) => {
            let handle = tm
                .upload()
                .bucket(bucket)
                .key(key)
                .body(InputStream::from_path(&args.source)?)
                .events(sink)
                .initiate()?;
            show_progress(stream).await?;
            handle.join().await?;
        }
        _ => return Err("exactly one of SOURCE and DEST must be s3://<bucket>/<key>".into()),
    }
    Ok(())
}

/// Splits `s3://<bucket>/<key>` into its bucket and key.
fn s3_uri(uri: &str) -> Option<(&str, &str)> {
    uri.strip_prefix("s3://")?.split_once('/')
}

/// Redraws every 100 ms until the stream ends, which it does once the transfer has sent its
/// `Finished`.
async fn show_progress(mut stream: TransferEventStream) -> std::io::Result<()> {
    let mut stdout = std::io::stdout();
    let mut view = None;
    loop {
        // Take every event queued since the last frame without waiting.
        let ended = loop {
            match stream.try_recv() {
                Ok(event) => {
                    if let TransferEvent::Started(started) = &event {
                        view = started.view().cloned();
                    }
                    writeln!(stdout, "{CLEAR_LINE}{}", line(&event))?;
                }
                Err(TryRecvError::Empty) => break false,
                // Every sink is gone, so no event will arrive.
                Err(_) => break true,
            }
        };
        if let Some(view) = &view {
            write!(stdout, "{CLEAR_LINE}{}", bar(view))?;
            stdout.flush()?;
        }
        if ended {
            return writeln!(stdout);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `upload started: <source> to <destination>`, and likewise for each outcome.
fn line(event: &TransferEvent) -> String {
    let transfer = event.transfer();
    let verb = match transfer.direction() {
        Direction::Upload => "upload",
        Direction::Download => "download",
        _ => "transfer",
    };
    let status = match event {
        TransferEvent::Started(_) => "started",
        TransferEvent::Finished(finished) => match finished.outcome() {
            Outcome::Succeeded { .. } => "succeeded",
            Outcome::Failed { .. } => "failed",
            Outcome::Cancelled { .. } => "cancelled",
            _ => "finished",
        },
        _ => "event",
    };
    format!(
        "{verb} {status}: {} to {}",
        transfer.source(),
        transfer.destination()
    )
}

/// `[#####   ]  62%` against a final total, or the bytes so far without one.
fn bar(view: &TransferView) -> String {
    let done = view.metrics().bytes_transferred;
    match view.byte_total() {
        Total::Final(total) if total > 0 => {
            let percent = done.min(total) * 100 / total;
            let filled = "#".repeat((percent * BAR_WIDTH / 100) as usize);
            format!(
                "[{filled:<width$}] {percent:>3}%",
                width = BAR_WIDTH as usize
            )
        }
        _ => format!("{done} bytes"),
    }
}
