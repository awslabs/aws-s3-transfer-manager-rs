/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! S3 Express sessions for directory buckets.
//!
//! Clients create a session with CreateSession (`GET /{bucket}?session`) and sign later requests to
//! that bucket with the session's credentials and token. s3s verifies every signature, with the
//! secret keys from [`Sessions::secret_key`], and serves CreateSession, which creates a session
//! with [`Sessions::create`]. Once s3s has verified a request's signature, [`check_access`] checks
//! that its credentials may make it.
//!
//! A directory bucket also accepts requests with the server's credentials and no session token.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use http::{HeaderMap, Method, Uri};
use s3s::{s3_error, S3Result};

use crate::bucket::BucketType;
use crate::error::Error;
use crate::s3s::ServerState;

/// How long a session's credentials are valid, as in S3.
const SESSION_DURATION: Duration = Duration::from_secs(5 * 60);

const SESSION_TOKEN_HEADER: &str = "x-amz-s3session-token";
const SESSION_TOKEN_QUERY: &str = "X-Amz-S3session-Token";

/// Sessions created with CreateSession, by access key.
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    sessions: Mutex<HashMap<String, Session>>,
}

impl Sessions {
    /// Create a session valid for `bucket` for five minutes, read-only if `read_only`, and
    /// return its credentials.
    pub(crate) fn create(&self, bucket: &str, read_only: bool) -> s3s::dto::SessionCredentials {
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
            secret_key: secret_key.clone(),
            bucket: bucket.to_owned(),
            read_only,
            expiration: SystemTime::now() + SESSION_DURATION,
        };
        let credentials = s3s::dto::SessionCredentials {
            access_key_id: access_key.clone(),
            expiration: session.expiration.into(),
            secret_access_key: secret_key,
            session_token: session.token.clone(),
        };
        self.sessions.lock().unwrap().insert(access_key, session);
        credentials
    }

    /// The secret key of the session with `access_key`, if there is one.
    pub(crate) fn secret_key(&self, access_key: &str) -> Option<String> {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(access_key)
            .map(|session| session.secret_key.clone())
    }

    /// Forget every session: requests signed with one fail with InvalidAccessKeyId.
    pub(crate) fn clear(&self) {
        self.sessions.lock().unwrap().clear();
    }
}

#[derive(Debug, Clone)]
struct Session {
    token: String,
    secret_key: String,
    /// The bucket the session is valid for.
    bucket: String,
    read_only: bool,
    expiration: SystemTime,
}

/// What [`check_access`] reads of a request whose signature s3s verified.
pub(crate) struct AccessRequest<'a> {
    /// The credentials s3s verified the signature with; `None` for an unsigned request.
    pub(crate) credentials: Option<&'a s3s::auth::Credentials>,
    /// The bucket the request is for, if any.
    pub(crate) bucket: Option<&'a str>,
    /// Whether the request is for an object, rather than a bucket or the service.
    pub(crate) object: bool,
    /// Whether the request is a CreateSession.
    pub(crate) create_session: bool,
    pub(crate) method: &'a Method,
    pub(crate) uri: &'a Uri,
    pub(crate) headers: &'a HeaderMap,
}

/// Check that the credentials of a request may make it, once s3s has verified its signature, if it
/// has one.
///
/// In any bucket, a request fails with:
/// - 403 AccessDenied if it is not signed;
/// - 400 InvalidArgument if it is signed both in its `Authorization` header and in its query;
/// - 400 AuthorizationHeaderMalformed (AuthorizationQueryParametersError if presigned) if it is
///   signed for `s3express` and is not for a directory bucket;
/// - 400 InvalidRequest if it has `x-amz-sdk-checksum-algorithm` but no checksum header or trailer;
/// - 403 InvalidAccessKeyId if it is signed with a session's credentials and is not for a directory
///   bucket.
///
/// In a directory bucket, signed for either service, a request fails with:
/// - 400 InvalidRequest if it is a CreateSession with a session token;
/// - 403 AccessDenied if it has the server's credentials and a session token;
/// - 403 InvalidAccessKeyId if it has a session's credentials with no session token or another
///   session's token;
/// - 400 InvalidRequest if its session token belongs to no session;
/// - 403 AccessDenied if its session has expired, is for another bucket, or is `ReadOnly` and the
///   request is not a GET or HEAD, or if the request is a bucket-level write (CreateBucket,
///   DeleteBucket, bucket configuration) signed with a session.
pub(crate) fn check_access(state: &ServerState, request: &AccessRequest<'_>) -> S3Result<()> {
    let Some(credentials) = request.credentials else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };
    let directory = request
        .bucket
        .is_some_and(|bucket| BucketType::of(bucket) == BucketType::Directory);
    let presigned = query_param(request.uri, "X-Amz-Algorithm").is_some();
    if presigned && request.headers.contains_key(http::header::AUTHORIZATION) {
        return Err(Error::InvalidArgument(if directory {
            "Only one auth mechanism allowed; only the X-Amz-Algorithm query parameter or the \
             Authorization header should be specified"
        } else {
            "Only one auth mechanism allowed; only the X-Amz-Algorithm query parameter, Signature \
             query string parameter or the Authorization header should be specified"
        })
        .into());
    }
    let scope = if presigned {
        query_param(request.uri, "X-Amz-Credential")
    } else {
        request
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(authorization_credential)
            .map(str::to_owned)
    };
    // The credential scope of the one signature the request carries, which s3s verified.
    let service = match scope.as_deref().and_then(scope_key_and_service) {
        Some((access_key, _)) if access_key != credentials.access_key => {
            return Err(Error::InvalidAccessKeyId.into());
        }
        Some((_, service)) => Some(service),
        None => None,
    };
    if service == Some("s3express") && !directory {
        return Err(if presigned {
            s3_error!(
                AuthorizationQueryParametersError,
                "Error parsing the X-Amz-Credential parameter; incorrect service \"s3express\". \
                 This endpoint belongs to \"s3\"."
            )
        } else {
            s3_error!(
                AuthorizationHeaderMalformed,
                "The authorization header is malformed; incorrect service \"s3express\". This \
                 endpoint belongs to \"s3\"."
            )
        });
    }
    check_checksum_headers(request.headers)?;
    let session = state
        .sessions
        .sessions
        .lock()
        .unwrap()
        .get(&credentials.access_key)
        .cloned();
    if !directory {
        return match session {
            Some(_) => Err(Error::InvalidAccessKeyId.into()),
            None => Ok(()),
        };
    }
    let token = request
        .headers
        .get(SESSION_TOKEN_HEADER)
        .map(|value| value.to_str().unwrap_or_default().to_owned())
        .or_else(|| query_param(request.uri, SESSION_TOKEN_QUERY));
    if request.create_session && token.is_some() {
        return Err(Error::InvalidRequest(
            "CreateSession request should not include \"x-amz-s3session-token\"",
        )
        .into());
    }
    match (session, token) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(Error::AccessDenied.into()),
        (Some(_), None) => Err(Error::InvalidAccessKeyId.into()),
        (Some(session), Some(token)) if session.token != token => {
            let known = state
                .sessions
                .sessions
                .lock()
                .unwrap()
                .values()
                .any(|other| other.token == token);
            if known {
                Err(Error::InvalidAccessKeyId.into())
            } else {
                Err(Error::InvalidRequest("Invalid session token in request").into())
            }
        }
        (Some(session), Some(_)) => {
            let bucket_level_write =
                !request.object && matches!(*request.method, Method::PUT | Method::DELETE);
            if request.bucket != Some(session.bucket.as_str())
                || SystemTime::now() > session.expiration
                || (session.read_only && !matches!(*request.method, Method::GET | Method::HEAD))
                || bucket_level_write
            {
                return Err(Error::AccessDenied.into());
            }
            Ok(())
        }
    }
}

/// The s3s access check: [`check_access`] for every request s3s serves, before its handler runs.
pub(crate) struct ExpressAccess {
    pub(crate) state: std::sync::Arc<ServerState>,
}

#[async_trait::async_trait]
impl s3s::access::S3Access for ExpressAccess {
    async fn check(&self, cx: &mut s3s::access::S3AccessContext<'_>) -> S3Result<()> {
        let (bucket, object) = match cx.s3_path() {
            s3s::path::S3Path::Root => (None, false),
            s3s::path::S3Path::Bucket { bucket } => (Some(&**bucket), false),
            s3s::path::S3Path::Object { bucket, .. } => (Some(&**bucket), true),
        };
        let request = AccessRequest {
            credentials: cx.credentials(),
            bucket,
            object,
            create_session: cx.s3_op().name() == "CreateSession",
            method: cx.method(),
            uri: cx.uri(),
            headers: cx.headers(),
        };
        check_access(&self.state, &request)
    }
}

/// The `Credential` field of a SigV4 `Authorization` header.
fn authorization_credential(authorization: &str) -> Option<&str> {
    authorization
        .strip_prefix("AWS4-HMAC-SHA256 ")?
        .split(',')
        .map(str::trim)
        .find_map(|field| field.strip_prefix("Credential="))
}

/// The access key and service of a SigV4 credential scope,
/// `<access key>/<date>/<region>/<service>/aws4_request`.
fn scope_key_and_service(credential: &str) -> Option<(&str, &str)> {
    let mut scope = credential.split('/');
    let access_key = scope.next()?;
    Some((access_key, scope.nth(2)?))
}

/// The decoded value of the query parameter `name` of `uri`.
fn query_param(uri: &Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|param| {
        let (key, value) = param.split_once('=').unwrap_or((param, ""));
        (key == name).then(|| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .into_owned()
        })
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn session(bucket: &str, read_only: bool, expiration: SystemTime) -> Session {
        Session {
            token: format!("token-{bucket}-{read_only}"),
            secret_key: "secret".to_owned(),
            bucket: bucket.to_owned(),
            read_only,
            expiration,
        }
    }

    #[test]
    fn reads_credential_scopes() {
        let credential = authorization_credential(
            "AWS4-HMAC-SHA256 Credential=AKID/20261017/us-east-1/s3express/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=abc",
        );
        assert_eq!(
            credential.and_then(scope_key_and_service),
            Some(("AKID", "s3express"))
        );
        assert_eq!(authorization_credential("AWS key:signature"), None);
        let uri: Uri = "/b?X-Amz-Credential=AKID%2F20261017%2Fus-east-1%2Fs3%2Faws4_request"
            .parse()
            .unwrap();
        let credential = query_param(&uri, "X-Amz-Credential");
        assert_eq!(
            credential.as_deref().and_then(scope_key_and_service),
            Some(("AKID", "s3"))
        );
    }

    const DIRECTORY: &str = "bucket--usw2-az1--x-s3";

    /// The error code of [`check_access`] for a request signed for `service` by `access_key`,
    /// with `token`, to `bucket` (and an object in it if `object`), or `None` if it may be made.
    #[allow(clippy::too_many_arguments)]
    fn access_error(
        state: &ServerState,
        service: &str,
        method: Method,
        bucket: Option<&str>,
        object: bool,
        access_key: &str,
        token: Option<&str>,
        extra: &[(&'static str, &str)],
    ) -> Option<String> {
        let mut headers = HeaderMap::new();
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/20261103/us-west-2/{service}/aws4_request, \
             SignedHeaders=host, Signature=0"
        );
        headers.insert(http::header::AUTHORIZATION, authorization.parse().unwrap());
        if let Some(token) = token {
            headers.insert(SESSION_TOKEN_HEADER, token.parse().unwrap());
        }
        for (name, value) in extra {
            headers.insert(*name, value.parse().unwrap());
        }
        let credentials = s3s::auth::Credentials {
            access_key: access_key.to_owned(),
            secret_key: "secret".to_owned().into(),
        };
        let uri: Uri = "/".parse().unwrap();
        let request = AccessRequest {
            credentials: Some(&credentials),
            bucket,
            object,
            create_session: false,
            method: &method,
            uri: &uri,
            headers: &headers,
        };
        check_access(state, &request)
            .err()
            .map(|error| error.code().as_str().to_owned())
    }

    #[test]
    fn checksum_algorithm_needs_a_checksum() {
        let state = ServerState::default();
        let algorithm = [("x-amz-sdk-checksum-algorithm", "CRC32")];
        for (service, bucket) in [("s3express", DIRECTORY), ("s3", "bucket")] {
            let error = access_error(
                &state,
                service,
                Method::PUT,
                Some(bucket),
                true,
                "akid",
                None,
                &algorithm,
            );
            assert_eq!(error.as_deref(), Some("InvalidRequest"), "{bucket}");
        }
    }

    /// An expired session, and a bucket-level write signed with a session, fail with AccessDenied.
    #[test]
    fn session_limits() {
        let state = ServerState::default();
        {
            let mut sessions = state.sessions.sessions.lock().unwrap();
            let valid = SystemTime::now() + SESSION_DURATION;
            sessions.insert("VALID".to_owned(), session(DIRECTORY, false, valid));
            let expired = SystemTime::now() - Duration::from_secs(1);
            let mut expired = session(DIRECTORY, false, expired);
            expired.token = "expired-token".to_owned();
            sessions.insert("EXPIRED".to_owned(), expired);
        }
        let token = |access_key: &str| {
            state.sessions.sessions.lock().unwrap()[access_key]
                .token
                .clone()
        };
        let check = |method, object, access_key: &str| {
            let token = token(access_key);
            access_error(
                &state,
                "s3express",
                method,
                Some(DIRECTORY),
                object,
                access_key,
                Some(&token),
                &[],
            )
        };
        assert_eq!(check(Method::GET, true, "VALID"), None);
        let denied = Some("AccessDenied".to_owned());
        assert_eq!(check(Method::GET, true, "EXPIRED"), denied);
        assert_eq!(check(Method::DELETE, false, "VALID"), denied);
    }
}
