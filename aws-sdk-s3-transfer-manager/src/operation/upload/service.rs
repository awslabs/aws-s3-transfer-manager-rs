/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// TODO(redux): This file contained the old Tower-based upload_part_service.
// Key concerns to preserve when implementing new scheduler-based execution:
// - upload_part_handler: SDK call with disable_payload_signing()
// - distribute_work: spawned N workers based on num_workers()
// - read_body: pulled from part_reader, spawned upload tasks
// - Tracing spans: upload-tasks, upload-read-tasks, upload-net-tasks, send-upload-part
// - ConcurrencyLimitLayer integration with old scheduler
// - Hedging policy for standard buckets (not directory buckets)
