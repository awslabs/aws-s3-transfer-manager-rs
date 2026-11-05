/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! S3 Express sessions for directory buckets.
//!
//! Clients of a directory bucket create a session with CreateSession and sign later requests with
//! the session's temporary credentials, for the service `s3express`, carrying the session token in
//! `x-amz-s3session-token`. s3s accepts only signatures for the `s3` service, so [`authenticate`]
//! handles these requests ahead of it: it serves CreateSession, checks that the credentials a
//! request names (the server's or a session's) may make it, and passes the request on to s3s
//! without its signature, marked as [`Verified`], which [`ExpressAccess`] admits. It does not
//! verify `s3express` signatures. Requests signed for `s3` pass through unchanged, and s3s
//! verifies them.
//!
//! As in S3, a directory bucket also accepts requests with the server's credentials and no
//! session, a read-only session allows only GET and HEAD requests, and a session is valid for the
//! bucket it was created for. A CreateSession that names no bucket (`GET /?session`, which a
//! client may send to the host of a path-style endpoint) creates a session valid for every
//! directory bucket.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use http::{Extensions, HeaderMap, Method, StatusCode};
use s3s::access::{S3Access, S3AccessContext};
use s3s::{Body, S3Result};

use crate::bucket::BucketType;
use crate::error::Error;
use crate::s3s::ServerState;
use crate::storage::StorageBackend;
use crate::Operation;

/// How long a session's credentials are valid, as in S3.
const SESSION_DURATION: Duration = Duration::from_secs(5 * 60);

const SESSION_TOKEN_HEADER: &str = "x-amz-s3session-token";
const SESSION_MODE_HEADER: &str = "x-amz-create-session-mode";

/// The credentials the server accepts: its own, given when it is built.
#[derive(Debug, Clone)]
pub(crate) struct ServerCredentials {
    pub(crate) access_key: &'static str,
}

/// Sessions created with CreateSession, by access key.
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    sessions: Mutex<HashMap<String, Session>>,
}

impl Sessions {
    /// Forget every session, so that requests signed with one fail with AccessDenied.
    pub(crate) fn clear(&self) {
        self.sessions.lock().unwrap().clear();
    }
}

#[derive(Debug, Clone)]
struct Session {
    token: String,
    /// The bucket the session is valid for, or `None` for every directory bucket.
    bucket: Option<String>,
    read_only: bool,
    expiration: SystemTime,
}

/// A request that [`authenticate`] lets through.
pub(crate) enum Authenticated {
    /// A request that s3s authenticates: one not signed for `s3express`, or signed for it and
    /// addressed to a general purpose bucket, whose signature s3s rejects.
    Unchanged(http::Request<Body>),
    /// A CreateSession whose credentials checked out, which the server answers itself.
    CreateSession(CreateSession),
    /// A request signed for `s3express` whose credentials checked out, with a chunked body
    /// decoded, and its signature replaced by the [`Verified`] mark.
    Verified(http::Request<Body>),
}

/// The mark of a request whose `s3express` credentials [`authenticate`] checked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Verified;

/// Whether `extensions` are those of a request that [`authenticate`] checked.
pub(crate) fn is_verified(extensions: &Extensions) -> bool {
    extensions.get::<Verified>().is_some()
}

/// The s3s access check: admits the requests whose signature s3s verified and those that
/// [`authenticate`] checked, which reach s3s without a signature.
pub(crate) struct ExpressAccess;

#[async_trait]
impl S3Access for ExpressAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        if cx.credentials().is_some() || is_verified(cx.extensions_mut()) {
            Ok(())
        } else {
            Err(s3s::s3_error!(AccessDenied, "Signature is required"))
        }
    }
}

/// Authenticate `request` ahead of the s3s service if it is signed for `s3express` and addressed
/// to a directory bucket, or a CreateSession that names no bucket: check that the credentials it
/// names, the server's or a session's, may make it. Fails with the error to answer the request
/// with.
pub(crate) async fn authenticate(
    state: &ServerState,
    credentials: &ServerCredentials,
    mut request: http::Request<Body>,
) -> crate::Result<Authenticated> {
    let Some(access_key) = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(s3express_access_key)
    else {
        return Ok(Authenticated::Unchanged(request));
    };
    let bucket = path_bucket(request.uri().path());
    let create_session =
        request.method() == Method::GET && has_query_param(request.uri(), "session");
    let directory = bucket
        .as_deref()
        .is_some_and(|bucket| BucketType::of(bucket) == BucketType::Directory);
    let bucketless_session = create_session && bucket.is_none();
    if !(directory || bucketless_session) {
        return Ok(Authenticated::Unchanged(request));
    }
    check_credentials(
        state,
        credentials,
        &request,
        &access_key,
        bucket.as_deref(),
        create_session,
    )?;
    check_checksum_headers(request.headers())?;
    if create_session {
        return Ok(Authenticated::CreateSession(CreateSession {
            request,
            bucket,
        }));
    }
    decode_chunked_payload(&mut request).await?;
    request.headers_mut().remove(http::header::AUTHORIZATION);
    request.extensions_mut().insert(Verified);
    Ok(Authenticated::Verified(request))
}

/// The access key of a SigV4 `Authorization` header whose credential scope is for `s3express`.
fn s3express_access_key(authorization: &str) -> Option<String> {
    let fields = authorization.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let credential = fields
        .split(',')
        .map(str::trim)
        .find_map(|field| field.strip_prefix("Credential="))?;
    let mut scope = credential.split('/');
    let access_key = scope.next()?;
    (scope.nth(2)? == "s3express").then(|| access_key.to_owned())
}

/// A CreateSession whose credentials checked out.
pub(crate) struct CreateSession {
    request: http::Request<Body>,
    /// The bucket the session is for, or `None` for every directory bucket.
    bucket: Option<String>,
}

impl CreateSession {
    /// Answer the CreateSession with a new session's credentials.
    pub(crate) async fn answer(
        self,
        state: &ServerState,
        storage: &dyn StorageBackend,
    ) -> http::Response<Body> {
        if let Err(error) = state.admit(Operation::CreateSession, self.bucket.as_deref(), None) {
            return error.into_response();
        }
        // As other requests, a CreateSession for a missing bucket fails with NoSuchBucket.
        if let Some(bucket) = &self.bucket {
            if let Err(error) = crate::s3s::check_bucket(storage, bucket).await {
                return error.into_response();
            }
        }
        let read_only = match header(self.request.headers(), SESSION_MODE_HEADER).as_deref() {
            None | Some("ReadWrite") => false,
            Some("ReadOnly") => true,
            Some(_) => {
                return Error::InvalidArgument("Invalid value for x-amz-create-session-mode")
                    .into_response()
            }
        };

        fn random() -> String {
            uuid::Uuid::new_v4()
                .simple()
                .to_string()
                .to_ascii_uppercase()
        }

        let access_key = format!("MOCKSESSION{}", &random()[..15]);
        let secret_key = format!("{}{}", random(), &random()[..8]);
        let session = Session {
            token: format!("{}{}{}", random(), random(), random()),
            bucket: self.bucket.clone(),
            read_only,
            expiration: SystemTime::now() + SESSION_DURATION,
        };
        let mut expiration = Vec::new();
        s3s::dto::Timestamp::from(session.expiration)
            .format(s3s::dto::TimestampFormat::DateTime, &mut expiration)
            .expect("a timestamp formats as a date-time");
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CreateSessionResult \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Credentials><SessionToken>{}\
             </SessionToken><SecretAccessKey>{}</SecretAccessKey><AccessKeyId>{}</AccessKeyId>\
             <Expiration>{}</Expiration></Credentials></CreateSessionResult>",
            session.token,
            secret_key,
            access_key,
            String::from_utf8(expiration).expect("a date-time is ASCII"),
        );
        state
            .sessions
            .sessions
            .lock()
            .unwrap()
            .insert(access_key, session);
        http::Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "application/xml")
            .body(Body::from(body.to_owned()))
            .expect("status and header are valid")
    }
}

/// Check that the credentials `access_key` names may make `request`.
///
/// As in S3, a request without a session token must name the server's credentials
/// (InvalidAccessKeyId otherwise), and a session's token must match it (InvalidAccessKeyId). A
/// CreateSession signed with a session, an unknown session, an expired session, a session for
/// another bucket, a `ReadOnly` session for anything but GET and HEAD, and a bucket-level write
/// (CreateBucket, DeleteBucket and the bucket configuration requests) signed with a session fail
/// with 403 AccessDenied.
fn check_credentials(
    state: &ServerState,
    credentials: &ServerCredentials,
    request: &http::Request<Body>,
    access_key: &str,
    bucket: Option<&str>,
    create_session: bool,
) -> crate::Result<()> {
    let token = request
        .headers()
        .get(SESSION_TOKEN_HEADER)
        .map(|value| value.to_str().unwrap_or_default().to_owned());
    let Some(token) = token else {
        if access_key == credentials.access_key {
            return Ok(());
        }
        return Err(Error::InvalidAccessKeyId);
    };
    if create_session {
        return Err(Error::AccessDenied);
    }
    let session = state
        .sessions
        .sessions
        .lock()
        .unwrap()
        .get(access_key)
        .cloned()
        .ok_or(Error::AccessDenied)?;
    if session.token != token {
        return Err(Error::InvalidAccessKeyId);
    }
    let bucket_level_write = !path_bucket_and_key(request.uri().path()).1
        && matches!(*request.method(), Method::PUT | Method::DELETE);
    if session.bucket.as_deref().is_some_and(|b| Some(b) != bucket)
        || SystemTime::now() > session.expiration
        || (session.read_only && !matches!(*request.method(), Method::GET | Method::HEAD))
        || bucket_level_write
    {
        return Err(Error::AccessDenied);
    }
    Ok(())
}

/// The checksum headers a request may carry.
const CHECKSUM_HEADERS: [&str; 5] = [
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
];

/// Fail, as S3 does, a request with `x-amz-sdk-checksum-algorithm` but no checksum header or
/// trailer for it.
fn check_checksum_headers(headers: &HeaderMap) -> crate::Result<()> {
    let has_checksum = CHECKSUM_HEADERS
        .iter()
        .any(|name| headers.contains_key(*name));
    if headers.contains_key("x-amz-sdk-checksum-algorithm")
        && !has_checksum
        && !headers.contains_key("x-amz-trailer")
    {
        return Err(Error::InvalidRequest(
            "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or \
             x-amz-trailer headers were found.",
        ));
    }
    Ok(())
}

/// Replace an `aws-chunked` body (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, `-TRAILER` or
/// `STREAMING-UNSIGNED-PAYLOAD-TRAILER`) by its data. Its trailing checksums become request
/// headers and the headers of the chunked encoding are removed, so that s3s serves the data and
/// checksums the client sent. Other bodies are left unchanged. s3s decodes such bodies only for
/// requests whose signature it verified. Chunk and trailer signatures are not checked, nor are the
/// declared decoded length and trailing headers.
///
/// The body is read in full before the request reaches its handler, so for these requests the
/// handler's errors, the throttle and faults, including `StallRequestRead`, apply only once the
/// client has sent the whole body.
async fn decode_chunked_payload(request: &mut http::Request<Body>) -> crate::Result<()> {
    let content_sha256 = header(request.headers(), "x-amz-content-sha256").unwrap_or_default();
    if !matches!(
        content_sha256.as_str(),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
            | "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    ) {
        return Ok(());
    }
    let body = request
        .body_mut()
        .store_all_limited(usize::MAX)
        .await
        .map_err(|_| Error::InvalidRequest("failed to read the request body"))?;
    let (data, trailers) = decode_chunks(&body)?;

    let headers = request.headers_mut();
    headers.remove("x-amz-decoded-content-length");
    headers.remove("x-amz-trailer");
    let encodings: Vec<String> = header(headers, "content-encoding")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|encoding| !encoding.is_empty() && *encoding != "aws-chunked")
        .map(str::to_owned)
        .collect();
    headers.remove(http::header::CONTENT_ENCODING);
    if !encodings.is_empty() {
        let value = http::HeaderValue::from_str(&encodings.join(","))
            .map_err(|_| Error::InvalidRequest("invalid content-encoding"))?;
        headers.insert(http::header::CONTENT_ENCODING, value);
    }
    // Only checksums: other trailing headers must not override the request's headers.
    for (name, value) in trailers
        .into_iter()
        .filter(|(name, _)| name.starts_with("x-amz-checksum-"))
    {
        let name = http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Error::InvalidRequest("invalid trailing header"))?;
        let value = http::HeaderValue::from_str(&value)
            .map_err(|_| Error::InvalidRequest("invalid trailing header"))?;
        headers.insert(name, value);
    }
    headers.insert(
        http::header::CONTENT_LENGTH,
        http::HeaderValue::from(data.len()),
    );
    headers.insert(
        "x-amz-content-sha256",
        http::HeaderValue::from_static("UNSIGNED-PAYLOAD"),
    );
    *request.body_mut() = Body::from(bytes::Bytes::from(data));
    Ok(())
}

/// Trailing headers of an `aws-chunked` body, as names in lowercase and values.
type TrailingHeaders = Vec<(String, String)>;

/// The data and trailing headers of an `aws-chunked` body: chunks of `<hex size>[;<extensions>]`,
/// CRLF, the data and CRLF, ending with a chunk of size 0, then trailing headers, one per line,
/// and an empty line. Chunk signatures, which are extensions, and the trailer signature, a
/// trailing header, are ignored.
fn decode_chunks(body: &[u8]) -> crate::Result<(Vec<u8>, TrailingHeaders)> {
    fn malformed() -> Error {
        Error::InvalidRequest("malformed aws-chunked body")
    }

    /// The next CRLF-terminated line of `rest`, which then starts after it.
    fn next_line(rest: &mut &[u8]) -> crate::Result<String> {
        let end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(malformed)?;
        let line = std::str::from_utf8(&rest[..end])
            .map_err(|_| malformed())?
            .to_owned();
        *rest = &rest[end + 2..];
        Ok(line)
    }

    let mut rest = body;
    let mut data = Vec::new();
    loop {
        let chunk_header = next_line(&mut rest)?;
        let size = chunk_header.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size, 16).map_err(|_| malformed())?;
        if size == 0 {
            break;
        }
        let end = size.checked_add(2).ok_or_else(malformed)?;
        if rest.len() < end || &rest[size..end] != b"\r\n" {
            return Err(malformed());
        }
        data.extend_from_slice(&rest[..size]);
        rest = &rest[end..];
    }
    let mut trailers = Vec::new();
    loop {
        let entry = next_line(&mut rest)?;
        if entry.is_empty() {
            break;
        }
        let (name, value) = entry.split_once(':').ok_or_else(malformed)?;
        let name = name.trim().to_ascii_lowercase();
        if name != "x-amz-trailer-signature" {
            trailers.push((name, value.trim().to_owned()));
        }
    }
    Ok((data, trailers))
}

/// The bucket of a path-style request path, if it names one.
fn path_bucket(path: &str) -> Option<String> {
    path_bucket_and_key(path).0
}

/// The bucket of a path-style request path, if it names one, and whether a key follows it. The
/// path is decoded before it is split, as s3s splits it, so that both find the same bucket.
fn path_bucket_and_key(path: &str) -> (Option<String>, bool) {
    let decoded = percent_encoding::percent_decode_str(path).decode_utf8_lossy();
    let mut segments = decoded.trim_start_matches('/').splitn(2, '/');
    let bucket = segments
        .next()
        .filter(|bucket| !bucket.is_empty())
        .map(str::to_owned);
    let has_key = segments.next().is_some_and(|key| !key.is_empty());
    (bucket, has_key)
}

fn has_query_param(uri: &http::Uri, name: &str) -> bool {
    uri.query().is_some_and(|query| {
        query
            .split('&')
            .any(|param| param.split('=').next() == Some(name))
    })
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREDENTIALS: ServerCredentials = ServerCredentials { access_key: "akid" };

    #[test]
    fn finds_s3express_access_keys() {
        assert_eq!(
            s3express_access_key(
                "AWS4-HMAC-SHA256 Credential=AKID/20261017/us-east-1/s3express/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc"
            )
            .as_deref(),
            Some("AKID")
        );
        assert_eq!(
            s3express_access_key(
                "AWS4-HMAC-SHA256 Credential=AKID/20261017/us-east-1/s3/aws4_request, \
                 SignedHeaders=host, Signature=abc"
            ),
            None
        );
        assert_eq!(s3express_access_key("AWS key:signature"), None);
    }

    #[test]
    fn decodes_chunks_and_trailers() {
        let signed = b"6;chunk-signature=a\r\nhello \r\n5;chunk-signature=b\r\nworld\r\n\
                       0;chunk-signature=c\r\n\r\n";
        let (data, trailers) = decode_chunks(signed).unwrap();
        assert_eq!(data, b"hello world");
        assert!(trailers.is_empty());

        let unsigned = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n";
        let signed_trailer = b"5;chunk-signature=a\r\nhello\r\n0;chunk-signature=b\r\n\
                               x-amz-checksum-crc32:NhCmhg==\r\nx-amz-trailer-signature:c\r\n\r\n";
        for body in [&unsigned[..], &signed_trailer[..]] {
            let (data, trailers) = decode_chunks(body).unwrap();
            assert_eq!(data, b"hello");
            assert_eq!(
                trailers,
                [("x-amz-checksum-crc32".to_owned(), "NhCmhg==".to_owned())]
            );
        }
    }

    #[test]
    fn rejects_malformed_chunks() {
        for body in [
            &b"zz\r\n"[..],
            b"5\r\nhel",
            b"5\r\nhelloXX0\r\n\r\n",
            b"0\r\nnot a header\r\n\r\n",
            b"ffffffffffffffff\r\nhello\r\n0\r\n\r\n",
        ] {
            assert!(
                matches!(decode_chunks(body), Err(Error::InvalidRequest(_))),
                "{body:?}"
            );
        }
    }

    /// `request` with an `Authorization` header signed for `s3express` by `access_key`, whose
    /// signature the server does not check.
    fn signed_by(mut request: http::Request<Body>, access_key: &str) -> http::Request<Body> {
        let value = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/20261103/us-west-2/s3express/aws4_request, \
             SignedHeaders=host, Signature=0"
        );
        request
            .headers_mut()
            .insert(http::header::AUTHORIZATION, value.parse().unwrap());
        request
    }

    fn request(method: Method, uri: &str) -> http::request::Builder {
        http::Request::builder()
            .method(method)
            .uri(uri)
            .header("host", "127.0.0.1")
    }

    const KEY: &str = "/bucket--usw2-az1--x-s3/key";

    #[tokio::test]
    async fn verified_requests_reach_s3s_unsigned_and_decoded() {
        let body =
            "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\ncontent-type:text/evil\r\n\r\n";
        let put = request(Method::PUT, KEY)
            .header("content-type", "text/plain")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "5")
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let Ok(Authenticated::Verified(mut request)) = authenticate(
            &ServerState::default(),
            &CREDENTIALS,
            signed_by(put, CREDENTIALS.access_key),
        )
        .await
        else {
            panic!("the request should be verified");
        };
        assert!(is_verified(request.extensions()));
        let headers = request.headers();
        assert!(!headers.contains_key(http::header::AUTHORIZATION));
        assert!(!headers.contains_key(http::header::CONTENT_ENCODING));
        assert_eq!(headers["x-amz-checksum-crc32"], "NhCmhg==");
        // Only checksums are taken from the trailing headers.
        assert_eq!(headers[http::header::CONTENT_TYPE], "text/plain");
        assert_eq!(headers[http::header::CONTENT_LENGTH], "5");
        let body = request
            .body_mut()
            .store_all_limited(usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"hello");
    }

    #[tokio::test]
    async fn checksum_algorithm_needs_a_checksum() {
        let put = request(Method::PUT, KEY)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("x-amz-sdk-checksum-algorithm", "CRC32")
            .body(Body::from("hello".to_owned()))
            .unwrap();
        let result = authenticate(
            &ServerState::default(),
            &CREDENTIALS,
            signed_by(put, CREDENTIALS.access_key),
        )
        .await;
        assert!(matches!(result, Err(Error::InvalidRequest(_))));
    }

    /// As in S3, an unknown or expired session, a session for another bucket, a `ReadOnly`
    /// session for a write and a bucket-level write signed with a session fail with
    /// AccessDenied; a wrong token, or unknown credentials without a session token, with
    /// InvalidAccessKeyId.
    #[tokio::test]
    async fn session_limits() {
        let state = ServerState::default();
        let session = |bucket: &str, read_only, expiration| Session {
            token: "token".to_owned(),
            bucket: Some(bucket.to_owned()),
            read_only,
            expiration,
        };
        let valid = SystemTime::now() + SESSION_DURATION;
        {
            let mut sessions = state.sessions.sessions.lock().unwrap();
            sessions.insert(
                "VALID".to_owned(),
                session("bucket--usw2-az1--x-s3", false, valid),
            );
            sessions.insert(
                "READ".to_owned(),
                session("bucket--usw2-az1--x-s3", true, valid),
            );
            sessions.insert(
                "OTHER".to_owned(),
                session("other--usw2-az1--x-s3", false, valid),
            );
            sessions.insert(
                "EXPIRED".to_owned(),
                session(
                    "bucket--usw2-az1--x-s3",
                    false,
                    SystemTime::now() - Duration::from_secs(1),
                ),
            );
        }
        let check = |method: Method, uri: &str, access_key: &str, token: Option<&str>| {
            let mut builder =
                request(method, uri).header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
            if let Some(token) = token {
                builder = builder.header(SESSION_TOKEN_HEADER, token);
            }
            let request = signed_by(builder.body(Body::empty()).unwrap(), access_key);
            let state = &state;
            async move { authenticate(state, &CREDENTIALS, request).await.err() }
        };
        assert!(check(Method::GET, KEY, "VALID", Some("token"))
            .await
            .is_none());
        assert!(check(Method::PUT, KEY, "VALID", Some("token"))
            .await
            .is_none());
        assert!(check(Method::GET, KEY, "akid", None).await.is_none());
        assert!(check(Method::GET, KEY, "READ", Some("token"))
            .await
            .is_none());
        for (method, uri, access_key) in [
            (Method::GET, KEY, "EXPIRED"),
            (Method::GET, KEY, "OTHER"),
            (Method::PUT, KEY, "READ"),
            (Method::DELETE, "/bucket--usw2-az1--x-s3", "VALID"),
            (Method::GET, KEY, "UNKNOWN"),
        ] {
            assert!(
                matches!(
                    check(method, uri, access_key, Some("token")).await,
                    Some(Error::AccessDenied)
                ),
                "{access_key} {uri}"
            );
        }
        assert!(matches!(
            check(Method::GET, KEY, "VALID", Some("other token")).await,
            Some(Error::InvalidAccessKeyId)
        ));
        assert!(matches!(
            check(Method::GET, KEY, "unknown", None).await,
            Some(Error::InvalidAccessKeyId)
        ));
    }

    #[test]
    fn finds_the_bucket_of_path_style_paths() {
        assert_eq!(path_bucket("/b--x-s3/key").as_deref(), Some("b--x-s3"));
        assert_eq!(path_bucket("/b--x-s3").as_deref(), Some("b--x-s3"));
        assert_eq!(path_bucket("/"), None);
        // As s3s, the path is decoded before it is split.
        assert_eq!(path_bucket("/gp%2Fkey--x-s3").as_deref(), Some("gp"));
        assert_eq!(
            path_bucket_and_key("/b--x-s3/key"),
            (Some("b--x-s3".to_owned()), true)
        );
        assert_eq!(
            path_bucket_and_key("/b--x-s3/"),
            (Some("b--x-s3".to_owned()), false)
        );
    }
}
