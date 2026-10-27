/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The HTTP service the server's connections serve requests with.
//!
//! [`MockService`] runs the checks that must happen ahead of the s3s service.

use std::sync::Arc;

use s3s::service::S3Service;
use s3s::{Body, HttpError};

use crate::express::{Authenticated, ServerCredentials};
use crate::s3s::ServerState;
use crate::storage::StorageBackend;

/// The s3s service with the steps around it.
#[derive(Clone)]
pub(crate) struct MockService {
    service: S3Service,
    state: Arc<ServerState>,
    storage: Arc<dyn StorageBackend>,
    credentials: ServerCredentials,
}

impl MockService {
    pub(crate) fn new(
        service: S3Service,
        state: Arc<ServerState>,
        storage: Arc<dyn StorageBackend>,
        credentials: ServerCredentials,
    ) -> Self {
        Self {
            service,
            state,
            storage,
            credentials,
        }
    }

    /// Serve one request. Some requests are answered here, ahead of the s3s service, which
    /// serves the rest:
    ///
    /// - A CreateSession is answered here.
    /// - A request signed for `s3express` is authenticated here. If that fails, it is answered
    ///   with the error; otherwise it is passed on to s3s without its signature, marked as
    ///   verified.
    pub(crate) async fn serve(
        &self,
        request: http::Request<Body>,
    ) -> Result<http::Response<Body>, HttpError> {
        let request =
            match crate::express::authenticate(&self.state, &self.credentials, request).await {
                Ok(Authenticated::Unchanged(request) | Authenticated::Verified(request)) => request,
                Ok(Authenticated::CreateSession(create_session)) => {
                    return Ok(create_session
                        .answer(&self.state, self.storage.as_ref())
                        .await)
                }
                Err(error) => return Ok(error.into_response()),
            };
        self.service.call(request).await
    }
}

impl hyper::service::Service<http::Request<hyper::body::Incoming>> for MockService {
    type Response = http::Response<Body>;
    type Error = HttpError;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, request: http::Request<hyper::body::Incoming>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move { service.serve(request.map(Body::from)).await })
    }
}
