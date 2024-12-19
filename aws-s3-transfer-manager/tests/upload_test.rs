/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cmp;
use std::task::ready;
use std::{task::Poll, time::Duration};

use aws_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_smithy_mocks_experimental::{mock, RuleMode};
use aws_smithy_runtime::client::http::test_util::infallible_client_fn;
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use bytes::Bytes;
use pin_project_lite::pin_project;
use test_common::mock_client_with_stubbed_http_client;
use tokio::sync::mpsc;

/// number of simultaneous uploads to create
const MANY_ASYNC_UPLOADS_CNT: usize = 200;
/// number of bytes to upload per transfer
const MANY_ASYNC_UPLOADS_OBJECT_SIZE: usize = 100;
/// bytes per write
const MANY_ASYNC_UPLOADS_BYTES_PER_WRITE: usize = 10;
/// how long to spend before assuming we're deadlocked
const SEND_DATA_TIMEOUT_S: u64 = 10;

pin_project! {
    #[derive(Debug)]
    struct TestStream {
        next_part_num: u64,
        rx: mpsc::Receiver<Bytes>,
        content_len: usize,
    }
}

impl TestStream {
    fn new(rx: mpsc::Receiver<Bytes>, content_len: usize) -> Self {
        Self {
            next_part_num: 1,
            rx,
            content_len,
        }
    }
}

impl PartStream for TestStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        let data = ready!(this.rx.poll_recv(cx));
        let part = data.map(|b| {
            let part_num = *this.next_part_num;
            *this.next_part_num += 1;
            Ok(PartData::new(part_num, b))
        });
        Poll::Ready(part)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.content_len as u64)
    }
}

fn mock_s3_client_for_multipart_upload() -> aws_sdk_s3::Client {
    let upload_id = "test-upload-id".to_owned();

    let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output({
        let upload_id = upload_id.clone();
        move || {
            CreateMultipartUploadOutput::builder()
                .upload_id(upload_id.clone())
                .build()
        }
    });

    let upload_part = mock!(aws_sdk_s3::Client::upload_part)
        .match_requests({
            let upload_id = upload_id.clone();
            move |input| input.upload_id.as_ref() == Some(&upload_id)
        })
        .then_output(|| UploadPartOutput::builder().build());

    let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
        .match_requests({
            let upload_id = upload_id.clone();
            move |r| r.upload_id.as_ref() == Some(&upload_id)
        })
        .then_output(|| CompleteMultipartUploadOutput::builder().build());

    mock_client_with_stubbed_http_client!(
        aws_sdk_s3,
        RuleMode::MatchAny,
        &[create_mpu, upload_part, complete_mpu]
    )
}

// Regression test for deadlock discovered by a user of Mountpoint
// The user opens MANY files at once. The user wrote data to some of the later files they opened,
// and waited for those writes to complete.
//
// If we wait on data from the first few files then both sides
// are waiting on each other causing deadlock.
//
// This test starts N uploads then only processes them starting from the last one created.
// If the test times out, then we suffer from deadlock.
//
// See https://github.com/awslabs/aws-c-s3/blob/5d8d4205e7de4e152bf26bb27d86f3acfa8cd5d2/tests/s3_many_async_uploads_without_data_test.c
#[tokio::test]
async fn test_many_uploads_no_deadlock() {
    let (_guard, _rx) = capture_test_logs();
    let client = mock_s3_client_for_multipart_upload();
    let client = aws_sdk_s3::Client::from_conf(
        client
            .config()
            .to_builder()
            .http_client(infallible_client_fn(|_req| {
                http_02x::Response::builder().status(200).body("").unwrap()
            }))
            .build(),
    );
    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .build();

    let tm = aws_s3_transfer_manager::Client::new(config);

    let mut transfers = Vec::with_capacity(MANY_ASYNC_UPLOADS_CNT);
    for i in 0..MANY_ASYNC_UPLOADS_CNT {
        let (tx, rx) = mpsc::channel(1);
        let stream = TestStream::new(rx, MANY_ASYNC_UPLOADS_OBJECT_SIZE);

        let handle = tm
            .upload()
            .bucket("test-bucket")
            .key(format!("many-async-uploads-{}.txt", i))
            .body(InputStream::from_part_stream(stream))
            .initiate()
            .unwrap();

        transfers.push((handle, tx));
    }

    let mut handles = Vec::with_capacity(MANY_ASYNC_UPLOADS_CNT);

    // process transfers in reverse order
    while let Some((handle, tx)) = transfers.pop() {
        let mut bytes_written = 0;
        let mut eof = false;
        while !eof {
            let wc = cmp::min(
                MANY_ASYNC_UPLOADS_BYTES_PER_WRITE,
                MANY_ASYNC_UPLOADS_OBJECT_SIZE - bytes_written,
            );
            eof = (bytes_written + wc) == MANY_ASYNC_UPLOADS_OBJECT_SIZE;

            let data = vec![b'z'; wc];
            let buf = Bytes::from(data);
            match tx
                .send_timeout(buf, Duration::from_secs(SEND_DATA_TIMEOUT_S))
                .await
            {
                Ok(_) => {}
                Err(err) => panic!("failed to send due to timeout or closed channel: {}", err),
            }
            bytes_written += wc;
        }

        drop(tx);
        handles.push(handle);
    }

    // wait for everything to finish
    while let Some(handle) = handles.pop() {
        handle.join().await.unwrap();
    }
}
