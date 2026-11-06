/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! RenameObject: `PUT /{bucket}/{key}?renameObject`, with the source key in `x-amz-rename-source`.
//! Only path-style requests are supported.
//!
//! [`RenameRoute`] serves renames as an s3s custom route, once s3s has verified their signature.
//! Renames to keys longer than 1,024 bytes fail with KeyTooLongError.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Extensions, HeaderMap, Method, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use crate::bucket::BucketType;
use crate::error::Error;
use crate::s3s::ServerState;
use crate::storage::{RenamePreconditions, RenameRequest, StorageBackend};
use crate::Operation;

const RENAME_QUERY_PARAM: &str = "renameObject";
const RENAME_SOURCE_HEADER: &str = "x-amz-rename-source";
const RENAME_SOURCE_IF_MATCH_HEADER: &str = "x-amz-rename-source-if-match";
const CLIENT_TOKEN_HEADER: &str = "x-amz-client-token";
/// The longest client token, in characters.
const MAX_CLIENT_TOKEN_LENGTH: usize = 64;
/// Rename conditions the server does not evaluate. A rename with any of them fails with
/// NotImplemented.
const UNSUPPORTED_CONDITION_HEADERS: &[&str] = &[
    "if-modified-since",
    "if-unmodified-since",
    "x-amz-rename-source-if-none-match",
    "x-amz-rename-source-if-modified-since",
    "x-amz-rename-source-if-unmodified-since",
];
/// The longest key, in bytes of UTF-8.
pub(crate) const MAX_KEY_LENGTH: usize = 1024;

/// Serves authenticated RenameObject requests.
pub(crate) struct RenameRoute {
    storage: Arc<dyn StorageBackend>,
    state: Arc<ServerState>,
}

impl RenameRoute {
    pub(crate) fn new(storage: Arc<dyn StorageBackend>, state: Arc<ServerState>) -> Self {
        Self { storage, state }
    }
}

#[async_trait]
impl S3Route for RenameRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        is_rename(method, uri)
    }

    /// Admit renames as [`crate::express::check_access`] admits other requests.
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        let (bucket, key) = crate::bucket::path_bucket_and_key(req.uri.path())?;
        let request = crate::express::AccessRequest {
            credentials: req.credentials.as_ref(),
            bucket: bucket.as_deref(),
            object: key.is_some(),
            create_session: false,
            method: &req.method,
            uri: &req.uri,
            headers: &req.headers,
        };
        crate::express::check_access(&self.state, &request)
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let (bucket, dst_key) = split_path(req.uri.path())?;
        let fault = self
            .state
            .admit(Operation::RenameObject, Some(&bucket), Some(&dst_key))?;
        if BucketType::of(&bucket) == BucketType::GeneralPurpose {
            return Err(Error::NotImplemented.into());
        }
        // The error reports the key's size and the limit, which an s3s error cannot carry.
        if dst_key.len() > MAX_KEY_LENGTH {
            return Ok(error_response(Error::KeyTooLong {
                size: dst_key.len(),
            }));
        }

        let header = |name: &str| {
            req.headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        for unsupported in UNSUPPORTED_CONDITION_HEADERS {
            if req.headers.contains_key(*unsupported) {
                return Err(Error::NotImplemented.into());
            }
        }
        // An empty token is no token.
        let client_token = client_token(&req.headers)?;
        let if_none_match = header("if-none-match");
        if if_none_match.as_deref().is_some_and(|value| value != "*") {
            return Err(Error::NotImplemented.into());
        }
        // The source key is percent-decoded, and may also be sent unencoded, so it need not be
        // ASCII.
        let src_key = req
            .headers
            .get(RENAME_SOURCE_HEADER)
            .ok_or(Error::InvalidRequest("Missing x-amz-rename-source"))?;
        let src_key = std::str::from_utf8(src_key.as_bytes())
            .map_err(|_| Error::InvalidArgument("x-amz-rename-source is not UTF-8"))?;
        // The source is the key as given: `/{bucket}/{key}` is a key, not a bucket and a key.
        let src_key = percent_decode(src_key)?;
        tracing::trace!(%bucket, %src_key, %dst_key, "RenameObject");
        crate::s3s::check_bucket(self.storage.as_ref(), &bucket).await?;

        let request = RenameRequest {
            src_key,
            dst_key,
            preconditions: RenamePreconditions {
                if_match: header("if-match"),
                if_none_match_any: if_none_match.is_some(),
                source_if_match: header(RENAME_SOURCE_IF_MATCH_HEADER),
            },
            client_token,
        };
        match self.storage.rename_object(&bucket, request).await {
            Ok(()) => {
                // WithholdResponseHeaders: the rename is applied; never respond.
                if let Some(crate::faults::FaultType::WithholdResponseHeaders) = fault {
                    std::future::pending::<()>().await;
                    unreachable!("pending future never resolves");
                }
                Ok(S3Response::new(Body::empty()))
            }
            // The error names the failed condition in a `Condition` element, which an s3s error
            // cannot carry, so the route builds the response.
            Err(error @ Error::RenamePreconditionFailed(_)) => Ok(error_response(error)),
            Err(error) => Err(error.into()),
        }
    }
}

/// The request's `x-amz-client-token`, if it has a non-empty one. Fails with 400 InvalidRequest for
/// a token longer than 64 characters or with characters outside ASCII 33 to 126.
fn client_token(headers: &HeaderMap) -> crate::Result<Option<String>> {
    let Some(token) = headers
        .get(CLIENT_TOKEN_HEADER)
        .map(|value| value.as_bytes())
        .filter(|token| !token.is_empty())
    else {
        return Ok(None);
    };
    if token.len() > MAX_CLIENT_TOKEN_LENGTH {
        return Err(Error::InvalidRequest(
            "Invalid header: Value for x-amz-client-token header is too long.",
        ));
    }
    if !token.iter().all(|byte| (33..=126).contains(byte)) {
        return Err(Error::InvalidRequest(
            "Invalid header: Value for x-amz-client-token header is invalid.",
        ));
    }
    Ok(Some(
        String::from_utf8(token.to_vec()).expect("an ASCII token is UTF-8"),
    ))
}

/// The route's response for `error`, with the elements an s3s error cannot carry
/// ([`Error::into_response`]).
fn error_response(error: Error) -> S3Response<Body> {
    let (parts, body) = error.into_response().into_parts();
    let mut response = S3Response::with_status(body, parts.status);
    response.headers = parts.headers;
    response
}

fn is_rename(method: &Method, uri: &Uri) -> bool {
    method == Method::PUT
        && uri.query().is_some_and(|query| {
            query
                .split('&')
                .any(|param| param.split('=').next() == Some(RENAME_QUERY_PARAM))
        })
}

/// The bucket and key of a path-style request path, read as s3s reads them
/// ([`crate::bucket::path_bucket_and_key`]).
fn split_path(path: &str) -> crate::Result<(String, String)> {
    match crate::bucket::path_bucket_and_key(path)? {
        (Some(bucket), Some(key)) => Ok((bucket, key)),
        _ => Err(Error::InvalidRequest(
            "Expected a path-style /{bucket}/{key} request",
        )),
    }
}

fn percent_decode(s: &str) -> crate::Result<String> {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| Error::InvalidUri)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_a_bucket_and_a_key() {
        assert!(split_path("/bucket").is_err());
        assert!(split_path("/bucket/").is_err());
    }

    #[test]
    fn matches_only_put_with_rename_query() {
        let route = RenameRoute::new(
            Arc::new(crate::storage::in_memory::InMemoryStorage::new()),
            Default::default(),
        );
        let mut ext = Extensions::new();
        let headers = HeaderMap::new();
        let matches = |method: Method, uri: &str, ext: &mut Extensions| {
            route.is_match(&method, &uri.parse().unwrap(), &headers, ext)
        };
        assert!(matches(Method::PUT, "/b/k?renameObject", &mut ext));
        assert!(matches(
            Method::PUT,
            "/b/k?x-id=RenameObject&renameObject=",
            &mut ext
        ));
        assert!(!matches(Method::GET, "/b/k?renameObject", &mut ext));
        assert!(!matches(Method::PUT, "/b/k?partNumber=1", &mut ext));
        assert!(!matches(Method::PUT, "/b/k?renameObjects", &mut ext));
    }
}
