/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Limit the maximum number of requests being concurrently processed by a service.
//!
//! This middleware is similar to the `tower::limit::concurrency` middleware but goes through
//! the transfer manager scheduler to control concurrency rather than just a semaphore.

mod future;
mod layer;
mod service;
