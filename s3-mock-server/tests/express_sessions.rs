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
    let content_length = body.len().to_string();
    let headers = [
        ("host", "localhost"),
        ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", "5"),
        ("x-amz-trailer", "x-amz-checksum-crc32"),
        ("content-length", content_length.as_str()),
    ];
    let request = signed_request("PUT", &format!("/{BUCKET}/key"), &headers, body);
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

/// An HTTP/1.1 request with `headers` and `body`, signed for `s3express` with the server's
/// credentials and no session, as a directory bucket accepts it.
fn signed_request(method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    signed_request_with(
        ("mock-akid", "mock-secret"),
        "s3express",
        method,
        path,
        headers,
        body,
    )
}

/// An HTTP/1.1 request with `headers` and `body`, signed for `service` with `credentials`
/// (access key, secret key).
fn signed_request_with(
    credentials: (&str, &str),
    service: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> String {
    use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
    let payload_hash = headers
        .iter()
        .find(|(name, _)| *name == "x-amz-content-sha256")
        .map_or("UNSIGNED-PAYLOAD", |(_, value)| *value)
        .to_owned();
    let identity =
        aws_credential_types::Credentials::new(credentials.0, credentials.1, None, None, "test")
            .into();
    let params = aws_sigv4::sign::v4::SigningParams::builder()
        .identity(&identity)
        .region("us-west-2")
        .name(service)
        .time(std::time::SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .unwrap()
        .into();
    let signable = SignableRequest::new(
        method,
        format!("http://localhost{path}"),
        headers.iter().copied(),
        SignableBody::Precomputed(payload_hash),
    )
    .unwrap();
    let (instructions, _) = sign(signable, &params).unwrap().into_parts();
    let (signing_headers, _) = instructions.into_parts();
    let mut request = format!("{method} {path} HTTP/1.1\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    for header in signing_headers {
        request.push_str(&format!("{}: {}\r\n", header.name(), header.value()));
    }
    request.push_str("\r\n");
    request.push_str(body);
    request
}

/// Sends `request` to the server and returns its response.
async fn send(handle: &s3_mock_server::ServerHandle, request: &str) -> Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(handle.socket_addr()).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

/// A session's credentials are valid with the session's token, whatever service the request is
/// signed for, and only in a directory bucket. Without the token, or in a general purpose bucket,
/// they fail with 403 InvalidAccessKeyId and store nothing.
#[tokio::test]
async fn test_session_credentials_need_token_and_directory_bucket() -> Result {
    let server = server().await;
    server.create_bucket("general-purpose").await?;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    let session = s3
        .create_session()
        .bucket(BUCKET)
        .send()
        .await?
        .credentials
        .expect("credentials");
    let headers = [
        ("host", "localhost"),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ("content-length", "5"),
    ];
    let with_token = [
        &headers[..],
        &[("x-amz-s3session-token", session.session_token.as_str())],
    ]
    .concat();
    let put = |bucket: &str, headers: &[(&str, &str)]| {
        signed_request_with(
            (&session.access_key_id, &session.secret_access_key),
            "s3",
            "PUT",
            &format!("/{bucket}/key"),
            headers,
            "hello",
        )
    };
    for (bucket, headers) in [(BUCKET, &headers[..]), ("general-purpose", &with_token[..])] {
        let response = send(&handle, &put(bucket, headers)).await?;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response.contains("<Code>InvalidAccessKeyId</Code>"),
            "{response}"
        );
        assert!(!server.object_exists(bucket, "key").await?, "{bucket}");
    }
    let response = send(&handle, &put(BUCKET, &with_token)).await?;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(server.object_exists(BUCKET, "key").await?);
    handle.shutdown().await?;
    Ok(())
}

/// A request to a general purpose bucket signed for S3 Express fails with 400
/// AuthorizationHeaderMalformed and stores nothing.
#[tokio::test]
async fn test_s3express_signature_in_general_purpose_bucket() -> Result {
    let server = server().await;
    server.create_bucket("general-purpose").await?;
    let handle = server.start().await?;
    let headers = [
        ("host", "localhost"),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ("content-length", "5"),
    ];
    let request = signed_request("PUT", "/general-purpose/key", &headers, "hello");
    let response = send(&handle, &request).await?;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        response.contains("<Code>AuthorizationHeaderMalformed</Code>"),
        "{response}"
    );
    assert!(!server.object_exists("general-purpose", "key").await?);
    handle.shutdown().await?;
    Ok(())
}

/// A request to a directory bucket with a wrong signature fails with 403 SignatureDoesNotMatch and
/// changes nothing: a PutObject stores nothing, and a CreateSession creates no session. Correctly
/// signed, the CreateSession succeeds.
#[tokio::test]
async fn test_wrong_signature() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let wrong = |request: &str| {
        let signature =
            request.find("Signature=").expect("the request is signed") + "Signature=".len();
        let mut wrong = request.as_bytes().to_vec();
        wrong[signature] = if wrong[signature] == b'0' { b'1' } else { b'0' };
        String::from_utf8(wrong).unwrap()
    };
    let put = signed_request(
        "PUT",
        &format!("/{BUCKET}/key"),
        &[
            ("host", "localhost"),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("content-length", "5"),
        ],
        "hello",
    );
    let create_session = signed_request(
        "GET",
        &format!("/{BUCKET}?session"),
        &[
            ("host", "localhost"),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ],
        "",
    );
    for request in [&put, &create_session] {
        let response = send(&handle, &wrong(request)).await?;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response.contains("<Code>SignatureDoesNotMatch</Code>"),
            "{response}"
        );
    }
    assert!(!server.object_exists(BUCKET, "key").await?);
    assert_eq!(server.request_count(Operation::CreateSession), 0);

    let response = send(&handle, &create_session).await?;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("<SessionToken>"), "{response}");
    assert_eq!(server.request_count(Operation::CreateSession), 1);
    handle.shutdown().await?;
    Ok(())
}

/// A request to a directory bucket without a signature fails with 403 AccessDenied and changes
/// nothing.
#[tokio::test]
async fn test_unsigned_requests_are_denied() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let request = format!(
        "PUT /{BUCKET}/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\
         Connection: close\r\n\r\nhello"
    );
    let response = send(&handle, &request).await?;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(response.contains("<Code>AccessDenied</Code>"), "{response}");
    assert!(!server.object_exists(BUCKET, "key").await?);
    handle.shutdown().await?;
    Ok(())
}

/// `reset` forgets the sessions: a client that holds one gets InvalidAccessKeyId, as with an
/// unknown session, and a new client creates another.
#[tokio::test]
async fn test_reset_forgets_sessions() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    round_trip(&s3).await?;
    server.reset().await?;
    server.create_bucket(BUCKET).await?;
    let fail = failure(s3.get_object().bucket(BUCKET).key("key").send().await);
    assert_failure(&fail, 403, "InvalidAccessKeyId");
    round_trip(&no_retry(&handle.client().await)).await?;
    handle.shutdown().await?;
    Ok(())
}

/// A CreateSession with a session token fails with 400 InvalidRequest and creates no session.
#[tokio::test]
async fn test_create_session_with_session() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    let session = s3
        .create_session()
        .bucket(BUCKET)
        .session_mode(SessionMode::ReadOnly)
        .send()
        .await?
        .credentials
        .expect("credentials");
    let headers = [
        ("host", "localhost"),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ("x-amz-s3session-token", &session.session_token),
    ];
    let request = signed_request_with(
        (&session.access_key_id, &session.secret_access_key),
        "s3express",
        "GET",
        &format!("/{BUCKET}?session"),
        &headers,
        "",
    );
    let response = send(&handle, &request).await?;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        response.contains(
            "<Message>CreateSession request should not include \
             &quot;x-amz-s3session-token&quot;</Message>"
        ),
        "{response}"
    );
    assert_eq!(server.request_count(Operation::CreateSession), 1);
    handle.shutdown().await?;
    Ok(())
}

/// A presigned URL `method path` with `session`'s credentials and token in its query, and an
/// `authorization` header if given.
fn presigned_request(
    session: &aws_sdk_s3::types::SessionCredentials,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: &str,
) -> String {
    use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SignatureLocation};
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    let identity = aws_credential_types::Credentials::new(
        &session.access_key_id,
        &session.secret_access_key,
        None,
        None,
        "test",
    )
    .into();
    let mut settings = aws_sigv4::http_request::SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(std::time::Duration::from_secs(60));
    let params = aws_sigv4::sign::v4::SigningParams::builder()
        .identity(&identity)
        .region("us-west-2")
        .name("s3express")
        .time(std::time::SystemTime::now())
        .settings(settings)
        .build()
        .unwrap()
        .into();
    let token = utf8_percent_encode(&session.session_token, NON_ALPHANUMERIC).to_string();
    let path = format!("{path}?X-Amz-S3session-Token={token}");
    let signable = SignableRequest::new(
        method,
        format!("http://localhost{path}"),
        [("host", "localhost")].into_iter(),
        SignableBody::UnsignedPayload,
    )
    .unwrap();
    let (instructions, _) = sign(signable, &params).unwrap().into_parts();
    let (_, query) = instructions.into_parts();
    let query: String = query
        .iter()
        .map(|(name, value)| format!("&{name}={}", utf8_percent_encode(value, NON_ALPHANUMERIC)))
        .collect();
    let authorization = authorization
        .map(|value| format!("authorization: {value}\r\n"))
        .unwrap_or_default();
    format!(
        "{method} {path}{query} HTTP/1.1\r\nhost: localhost\r\nConnection: close\r\n\
         content-length: {}\r\n{authorization}\r\n{body}",
        body.len()
    )
}

/// A presigned URL with a session's credentials and token is served, within the session's limits: a
/// read-only session's PUT fails with 403 AccessDenied. A presigned URL with an `Authorization`
/// header too fails with 400 InvalidArgument. Neither stores anything.
#[tokio::test]
async fn test_presigned_with_session() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let s3 = no_retry(&handle.client().await);
    s3.put_object()
        .bucket(BUCKET)
        .key("key")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await?;
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
    let read_only = session(SessionMode::ReadOnly).await;
    let path = format!("/{BUCKET}/new");
    let get = presigned_request(&read_only, "GET", &format!("/{BUCKET}/key"), None, "");
    let response = send(&handle, &get).await?;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let response = send(
        &handle,
        &presigned_request(&read_only, "PUT", &path, None, "hello"),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(response.contains("<Code>AccessDenied</Code>"), "{response}");
    let read_write = session(SessionMode::ReadWrite).await;
    let authorization = "AWS4-HMAC-SHA256 Credential=mock-akid/20260101/us-west-2/s3express/\
                         aws4_request, SignedHeaders=host, Signature=00";
    let request = presigned_request(&read_write, "PUT", &path, Some(authorization), "hello");
    let response = send(&handle, &request).await?;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        response.contains("<Code>InvalidArgument</Code>"),
        "{response}"
    );
    assert!(!server.object_exists(BUCKET, "new").await?);
    handle.shutdown().await?;
    Ok(())
}

/// CreateSession returns temporary credentials that expire in five minutes. It fails with
/// NoSuchBucket for a missing bucket and with 400 InvalidArgument for an unknown session mode. Its
/// requests are counted and draw faults.
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

    let mode = SessionMode::from("ReadAppend");
    let fail = failure(
        s3.create_session()
            .bucket(BUCKET)
            .session_mode(mode)
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidArgument");
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

/// Requests signed with a session's credentials and token are served, as are requests with the
/// server's credentials and no session. A session is valid only with its own token and for its own
/// bucket, and a read-only session only for reads; a token of no session fails with 400
/// InvalidRequest.
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
    let fail = failure(get(with_session(&read_write, "bogus")).await);
    assert_failure(&fail, 400, "InvalidRequest");
    let fail = failure(get(signing_with(&s3, base, Some("bogus".to_owned()))).await);
    assert_failure(&fail, 403, "AccessDenied");
    let unknown = Credentials::new("unknown", "mock-secret", None, None, "unknown");
    let fail = failure(get(signing_with(&s3, unknown, None)).await);
    assert_failure(&fail, 403, "InvalidAccessKeyId");
    handle.shutdown().await?;
    Ok(())
}

/// A CreateSession that names no bucket, as a client may send it to a path-style endpoint, is
/// not served: signed for `s3express`, a request to no bucket fails with 400
/// AuthorizationHeaderMalformed, as one to a general purpose bucket.
#[tokio::test]
async fn test_session_without_bucket() -> Result {
    let server = server().await;
    let handle = server.start().await?;
    let http = handle.client().await;
    let base = Credentials::new("mock-akid", "mock-secret", None, None, "base");
    let s3 = signing_with(&http, base, None);
    // `GET /?session`: the SDK names the bucket in the path, so the request is rewritten to
    // address the host only.
    let result = s3
        .create_session()
        .bucket(BUCKET)
        .customize()
        .mutate_request(|request| {
            let uri = request.uri().replace(&format!("/{BUCKET}"), "");
            request.set_uri(uri).unwrap();
        })
        .send()
        .await;
    assert_failure(&failure(result), 400, "AuthorizationHeaderMalformed");
    assert_eq!(server.request_count(Operation::CreateSession), 0);
    handle.shutdown().await?;
    Ok(())
}

/// PutObject and UploadPart with streaming bodies, which the SDK sends chunked with the checksum in
/// a trailer, store the data; PutObject also stores the trailing checksum.
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
