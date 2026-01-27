/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Integration tests for aws-sdk-s3-transfer-manager.
//!
//! These tests use s3-mock-server to test the full transfer manager flow
//! against a real HTTP server, validating the public API without mocking internals.

pub mod upload;
