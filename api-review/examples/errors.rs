//! Report, classify, and act on transfer errors.
//!
//! Each `fails_*` function triggers one kind of failure the way a customer's code would meet it.
//! `report()` and `classify()` are the customer's error handling. `main` runs every failure against a
//! local S3 endpoint and prints what each handler sees.

#[path = "support/mock.rs"]
mod mock;

use std::path::Path;

use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3_transfer_manager::Client;
use aws_sdk_s3_transfer_manager::error::{Error, ErrorKind};
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use mock::BUCKET;

/// Log an error the two ways customers commonly do.
fn report(e: &Error) -> String {
    format!("    Display:             {e}\n    DisplayErrorContext: {}", DisplayErrorContext(e))
}

/// Decide what to do with an error.
fn classify(e: &Error) -> String {
    match e.kind() {
        ErrorKind::ServiceError if e.is_not_found() => format!("not found ({:?})", e.code()),
        ErrorKind::ServiceError if e.code() == Some("PreconditionFailed") => "condition not met".into(),
        ErrorKind::ServiceError => format!(
            "S3 rejected {:?} with {:?} (request id {:?})",
            e.operation_name(),
            e.code(),
            e.request_id()
        ),
        ErrorKind::IntegrityError(ie) => format!("checksum mismatch: {:?}", ie.algorithm()),
        ErrorKind::ChildOperationFailed => {
            let failed = e.failed_uploads().map(<[_]>::len).or(e.failed_downloads().map(<[_]>::len));
            format!("bulk transfer stopped; {failed:?} objects failed")
        }
        ErrorKind::InputInvalid => "fix the request".into(),
        ErrorKind::IOError => "local or transport I/O failed".into(),
        ErrorKind::OperationCancelled => "cancelled".into(),
        other => format!("other: {other:?}"),
    }
}

/// Download a key that doesn't exist.
async fn fails_missing_key(client: &Client) -> Error {
    let mut handle = client.download().bucket(BUCKET).key("no/such/key").initiate().expect("starts");
    while let Some(chunk) = handle.body_mut().next().await {
        if chunk.is_err() {
            break;
        }
    }
    handle.join().await.expect_err("missing key")
}

/// Upload the same key twice with a create-only condition.
async fn fails_precondition(client: &Client) -> Error {
    let upload = || {
        client
            .upload()
            .bucket(BUCKET)
            .key("once.txt")
            .body(InputStream::from("first"))
            .if_none_match("*")
            .initiate()
            .expect("starts")
    };
    upload().join().await.expect("first upload succeeds");
    upload().join().await.expect_err("second upload is rejected")
}

/// Upload from a file that doesn't exist.
async fn fails_missing_file(client: &Client, path: &Path) -> Error {
    let body = match InputStream::from_path(path) {
        Ok(body) => body,
        // `from_path` returns `io::error::Error`; converting gives the transfer manager's `Error`.
        Err(e) => return Error::from(e),
    };
    client.upload().bucket(BUCKET).key("k").body(body).initiate().expect("starts").join().await.expect_err("fails")
}

/// Download a bucket into a directory that doesn't exist.
async fn fails_missing_destination(client: &Client, dest: &Path) -> Error {
    let handle = client.download_objects().bucket(BUCKET).destination(dest).initiate().expect("starts");
    handle.join().await.expect_err("missing destination")
}

/// Upload a directory containing an unreadable file, under the default policy.
async fn fails_bulk_child(client: &Client, dir: &Path) -> Error {
    let handle = client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir)
        .walker(FsWalker::builder().recursive(true).build())
        .initiate()
        .expect("starts");
    handle.join().await.expect_err("unreadable file")
}

#[tokio::main]
async fn main() {
    let mock = mock::Mock::start().await;
    let client = mock.client().await;
    let root = std::env::temp_dir().join(format!("tm-api-review-errors-{}", std::process::id()));
    std::fs::create_dir_all(root.join("bulk")).expect("temp dir");
    std::fs::write(root.join("bulk/unreadable.txt"), b"x").expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root.join("bulk/unreadable.txt"), std::fs::Permissions::from_mode(0o000))
            .expect("chmod");
    }
    mock.put("seed.txt", "seed").await;

    let cases = [
        ("missing key", fails_missing_key(&client).await),
        ("precondition", fails_precondition(&client).await),
        ("missing file", fails_missing_file(&client, &root.join("nope.bin")).await),
        ("missing destination", fails_missing_destination(&client, &root.join("not-created")).await),
        ("bulk child", fails_bulk_child(&client, &root.join("bulk")).await),
    ];
    for (name, e) in &cases {
        println!("{name}: kind={:?} -> {}\n{}", e.kind(), classify(e), report(e));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(root.join("bulk/unreadable.txt"), std::fs::Permissions::from_mode(0o644));
    }
    let _ = std::fs::remove_dir_all(&root);
}
