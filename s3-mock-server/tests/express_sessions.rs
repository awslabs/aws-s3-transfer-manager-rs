// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! S3 Express session authentication for directory buckets: CreateSession, and requests signed
//! with a session's credentials, as the SDK sends them.

mod common;

use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::SessionMode;
use aws_sdk_s3::Client;
use common::{assert_failure, failure, no_retry, Result};
use s3_mock_server::{Error, FaultTarget, FaultType, Occurrence, Operation, S3MockServer};

const BUCKET: &str = "bucket--usw2-az1--x-s3";
const OTHER_BUCKET: &str = "other--usw2-az1--x-s3";

async fn server() -> S3MockServer {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build server");
    server.create_bucket(BUCKET).await.expect("create bucket");
    server
        .create_bucket(OTHER_BUCKET)
        .await
        .expect("create bucket");
    server
}

/// Put, get, list and delete an object in the directory bucket.
async fn round_trip(s3: &Client) -> Result {
    s3.put_object()
        .bucket(BUCKET)
        .key("dir/key")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await?;
    let body = s3
        .get_object()
        .bucket(BUCKET)
        .key("dir/key")
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(&body[..], b"hello");
    let listed = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("dir/")
        .send()
        .await?;
    assert_eq!(listed.key_count, Some(1));
    s3.delete_object()
        .bucket(BUCKET)
        .key("dir/key")
        .send()
        .await?;
    Ok(())
}

/// The SDK creates a session for a directory bucket and signs its requests with it; one session
/// serves every request until it expires.
#[tokio::test]
async fn test_sdk_uses_sessions() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    round_trip(&no_retry(&handle.client().await)).await?;
    assert_eq!(server.request_count(Operation::CreateSession), 1);
    assert_eq!(server.request_count(Operation::PutObject), 1);
    handle.shutdown().await?;
    Ok(())
}

/// A chunked upload's trailing checksum is checked as a checksum header is: one that does not
/// match the data fails with 400 BadDigest and stores nothing.
#[tokio::test]
async fn test_wrong_trailing_checksum() -> Result {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = server().await;
    let handle = server.start().await?;
    // The CRC32 of `jello`, sent with `hello`.
    let body = "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:TND15g==\r\n\r\n";
    let request = format!(
        "PUT /{BUCKET}/key HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential=mock-akid/20261104/us-west-2/s3express/aws4_request, \
         SignedHeaders=host, Signature=0\r\n\
         x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER\r\nContent-Encoding: aws-chunked\r\n\
         x-amz-decoded-content-length: 5\r\nx-amz-trailer: x-amz-checksum-crc32\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(handle.socket_addr()).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(response.contains("<Code>BadDigest</Code>"), "{response}");
    assert!(!server.object_exists(BUCKET, "key").await?);
    handle.shutdown().await?;
    Ok(())
}

/// A request to a directory bucket without a signature fails with 403 AccessDenied and changes
/// nothing.
#[tokio::test]
async fn test_unsigned_requests_are_denied() -> Result {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = server().await;
    let handle = server.start().await?;
    for request in [
        format!("PUT /{BUCKET}/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"),
        format!("PUT /{BUCKET}/key?renameObject HTTP/1.1\r\nHost: localhost\r\nx-amz-rename-source: other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
    ] {
        let mut stream = tokio::net::TcpStream::connect(handle.socket_addr()).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(response.contains("<Code>AccessDenied</Code>"), "{response}");
    }
    assert!(!server.object_exists(BUCKET, "key").await?);
    handle.shutdown().await?;
    Ok(())
}

/// `reset` forgets the sessions: a client that holds one gets AccessDenied, and a new client
/// creates another.
#[tokio::test]
async fn test_reset_forgets_sessions() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    round_trip(&s3).await?;
    server.reset().await?;
    server.create_bucket(BUCKET).await?;
    let fail = failure(s3.get_object().bucket(BUCKET).key("key").send().await);
    assert_failure(&fail, 403, "AccessDenied");
    round_trip(&no_retry(&handle.client().await)).await?;
    handle.shutdown().await?;
    Ok(())
}

/// CreateSession returns temporary credentials that expire in five minutes, as S3's do, and
/// fails with NoSuchBucket for a missing bucket. Its requests are counted
/// and draw faults.
#[tokio::test]
async fn test_create_session() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    let session = s3.create_session().bucket(BUCKET).send().await?;
    let credentials = session.credentials.expect("credentials");
    assert!(!credentials.access_key_id.is_empty());
    assert!(!credentials.secret_access_key.is_empty());
    assert!(!credentials.session_token.is_empty());
    let remaining = credentials.expiration.secs()
        - aws_sdk_s3::primitives::DateTime::from(std::time::SystemTime::now()).secs();
    assert!((290..=300).contains(&remaining), "{remaining}");

    // As other requests, CreateSession for a missing bucket fails with NoSuchBucket.
    let fail = failure(
        s3.create_session()
            .bucket("missing--usw2-az1--x-s3")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");

    server.insert_fault_for(
        FaultTarget::operation(Operation::CreateSession).bucket(BUCKET),
        FaultType::Error(Error::SlowDown),
        0,
        Occurrence::Once,
    );
    let fail = failure(s3.create_session().bucket(BUCKET).send().await);
    assert_failure(&fail, 503, "SlowDown");
    assert_eq!(server.request_count(Operation::CreateSession), 3);
    handle.shutdown().await?;
    Ok(())
}

/// A client like `base` signing with the given credentials, without S3 Express sessions, that
/// adds `token` as the session token.
fn signing_with(base: &Client, credentials: Credentials, token: Option<String>) -> Client {
    let config = base
        .config()
        .to_builder()
        .credentials_provider(credentials)
        .disable_s3_express_session_auth(true)
        .build();
    let client = no_retry(&Client::from_conf(config));
    match token {
        None => client,
        Some(token) => Client::from_conf(
            client
                .config()
                .to_builder()
                .interceptor(AddToken(token))
                .build(),
        ),
    }
}

/// Adds `x-amz-s3session-token` to every request before it is signed.
#[derive(Debug)]
struct AddToken(String);

impl aws_sdk_s3::config::Intercept for AddToken {
    fn name(&self) -> &'static str {
        "AddToken"
    }

    fn modify_before_signing(
        &self,
        context: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> std::result::Result<(), aws_sdk_s3::error::BoxError> {
        context
            .request_mut()
            .headers_mut()
            .insert("x-amz-s3session-token", self.0.clone());
        Ok(())
    }
}

/// Requests signed with a session's credentials and token are served, also without a session
/// with the server's credentials, as in S3. A session is valid only with its own token and for
/// its own bucket, and a read-only session only for reads.
#[tokio::test]
async fn test_session_authorization() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    let session = |mode| {
        let s3 = s3.clone();
        async move {
            s3.create_session()
                .bucket(BUCKET)
                .session_mode(mode)
                .send()
                .await
                .unwrap()
                .credentials
                .unwrap()
        }
    };
    let read_write = session(SessionMode::ReadWrite).await;
    let read_only = session(SessionMode::ReadOnly).await;
    let other = s3
        .create_session()
        .bucket(OTHER_BUCKET)
        .send()
        .await?
        .credentials
        .unwrap();
    let with_session = |credentials: &aws_sdk_s3::types::SessionCredentials, token: &str| {
        signing_with(
            &s3,
            Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "session",
            ),
            Some(token.to_owned()),
        )
    };
    let put = |client: Client| async move {
        client
            .put_object()
            .bucket(BUCKET)
            .key("key")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
    };
    let get =
        |client: Client| async move { client.get_object().bucket(BUCKET).key("key").send().await };

    put(with_session(&read_write, &read_write.session_token)).await?;
    get(with_session(&read_only, &read_only.session_token)).await?;
    let base = Credentials::new("mock-akid", "mock-secret", None, None, "base");
    get(signing_with(&s3, base.clone(), None)).await?;

    let fail = failure(put(with_session(&read_only, &read_only.session_token)).await);
    assert_failure(&fail, 403, "AccessDenied");
    let fail = failure(get(with_session(&other, &other.session_token)).await);
    assert_failure(&fail, 403, "AccessDenied");
    let fail = failure(get(with_session(&read_write, &read_only.session_token)).await);
    assert_failure(&fail, 403, "InvalidAccessKeyId");
    let fail = failure(get(signing_with(&s3, base, Some("bogus".to_owned()))).await);
    assert_failure(&fail, 403, "AccessDenied");
    let unknown = Credentials::new("unknown", "mock-secret", None, None, "unknown");
    let fail = failure(get(signing_with(&s3, unknown, None)).await);
    assert_failure(&fail, 403, "InvalidAccessKeyId");
    handle.shutdown().await?;
    Ok(())
}

/// A CreateSession that names no bucket, as a client may send it to a path-style endpoint,
/// creates a session valid for every directory bucket.
#[tokio::test]
async fn test_session_without_bucket() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let http = handle.client().await;
    let base = Credentials::new("mock-akid", "mock-secret", None, None, "base");
    let s3 = signing_with(&http, base, None);
    // `GET /?session`: the SDK names the bucket in the path, so the request is rewritten to
    // address the host only.
    let response = s3
        .create_session()
        .bucket(BUCKET)
        .customize()
        .mutate_request(|request| {
            let uri = request.uri().replace(&format!("/{BUCKET}"), "");
            request.set_uri(uri).unwrap();
        })
        .send()
        .await?;
    let credentials = response.credentials.unwrap();
    for bucket in [BUCKET, OTHER_BUCKET] {
        let client = signing_with(
            &http,
            Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "session",
            ),
            Some(credentials.session_token.clone()),
        );
        client
            .put_object()
            .bucket(bucket)
            .key("key")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await?;
    }
    handle.shutdown().await?;
    Ok(())
}

/// PutObject and UploadPart with streaming bodies, which the SDK sends chunked with the checksum
/// in a trailer, store the data and the trailing checksum.
#[tokio::test]
async fn test_streaming_uploads() -> Result {
    use aws_sdk_s3::types::{
        ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart,
    };
    let server = server().await;
    let handle = server.start().await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("body");
    let content: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &content)?;
    let s3 = no_retry(&handle.client().await);
    s3.put_object()
        .bucket(BUCKET)
        .key("streamed")
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .body(ByteStream::from_path(&path).await?)
        .send()
        .await?;
    assert_eq!(
        server.get_object(BUCKET, "streamed").await?.unwrap().body,
        content
    );
    let head = s3
        .head_object()
        .bucket(BUCKET)
        .key("streamed")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc32_c, Some(common::crc32c(&content)));

    let upload_id = s3
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mpu")
        .send()
        .await?
        .upload_id
        .unwrap();
    let part = s3
        .upload_part()
        .bucket(BUCKET)
        .key("mpu")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_path(&path).await?)
        .send()
        .await?;
    s3.complete_multipart_upload()
        .bucket(BUCKET)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag.unwrap())
                        .set_checksum_crc64_nvme(part.checksum_crc64_nvme)
                        .set_checksum_crc32(part.checksum_crc32)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    assert_eq!(
        server.get_object(BUCKET, "mpu").await?.unwrap().body,
        content
    );
    handle.shutdown().await?;
    Ok(())
}
