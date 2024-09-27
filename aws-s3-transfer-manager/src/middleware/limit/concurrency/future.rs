/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::runtime::scheduler::OwnedWorkPermit;

use pin_project_lite::pin_project;
use std::future::Future;

pin_project! {
    #[derive(Debug)]
    pub(crate) struct ResponseFuture<T> {
        #[pin]
        inner: T,
        // retain until dropped when future completes
        _permit: OwnedWorkPermit
    }
}

impl<T> ResponseFuture<T> {
    pub(crate) fn new(inner: T, _permit: OwnedWorkPermit) -> ResponseFuture<T> {
        ResponseFuture { inner, _permit }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        todo!()
    }
}
