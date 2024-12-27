/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{str::FromStr, task::Poll};

use aws_s3_transfer_manager::{
    io::{InputStream, PartData, PartStream, SizeHint},
    operation::upload::ChecksumStrategy,
    types::{ConcurrencySetting, PartSize},
};
use aws_sdk_s3::{
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput,
        create_multipart_upload::CreateMultipartUploadOutput, upload_part::UploadPartOutput,
    },
    types::{ChecksumAlgorithm, ChecksumType},
};
use aws_smithy_mocks_experimental::{mock, Rule, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use bytes::Bytes;
use pin_project_lite::pin_project;
use test_common::mock_client_with_stubbed_http_client;

const MEBIBYTE: usize = 1024 * 1024;

fn calculate_checksum(algorithm: &ChecksumAlgorithm, data: &[u8]) -> String {
    let smithy_algorithm =
        aws_smithy_checksums::ChecksumAlgorithm::from_str(algorithm.as_str()).unwrap();
    let mut http_checksum = smithy_algorithm.into_impl();
    http_checksum.update(data);
    http_checksum.header_value().to_str().unwrap().to_string()
}

fn calculate_full_object_checksum(algorithm: &ChecksumAlgorithm, parts: &Vec<Bytes>) -> String {
    // S3 doesn't allow SHA algorithms to do full-object checksums with multipart upload
    assert_ne!(algorithm, &ChecksumAlgorithm::Sha1);
    assert_ne!(algorithm, &ChecksumAlgorithm::Sha256);

    let smithy_algorithm =
        aws_smithy_checksums::ChecksumAlgorithm::from_str(algorithm.as_str()).unwrap();
    let mut http_checksum = smithy_algorithm.into_impl();
    for part in parts {
        http_checksum.update(part.as_ref());
    }
    http_checksum.header_value().to_str().unwrap().to_string()
}

fn calculate_composite_checksum(algorithm: &ChecksumAlgorithm, parts: &Vec<Bytes>) -> String {
    // S3 doesn't allow CRC64-NVME to do composite checksums
    assert_ne!(algorithm, &ChecksumAlgorithm::Crc64Nvme);

    let mut concatenated_part_checksums = String::new();
    for part in parts {
        let checksum = calculate_checksum(algorithm, part.as_ref());
        concatenated_part_checksums.push_str(&checksum);
    }

    let composite_checksum = calculate_checksum(algorithm, concatenated_part_checksums.as_bytes());
    let count = parts.len();
    format!("{composite_checksum}-{count}")
}

/// Calculate final Composite or FullObject checksum for multipart upload
fn calculate_multipart_checksum(
    algorithm: &ChecksumAlgorithm,
    checksum_type: &ChecksumType,
    parts: &Vec<Bytes>,
) -> String {
    match checksum_type {
        ChecksumType::Composite => calculate_composite_checksum(algorithm, parts),
        ChecksumType::FullObject => calculate_full_object_checksum(algorithm, parts),
        _ => panic!("unrecognized checksum type"),
    }
}

/// Calculate ETag as it appears in S3 headers, with the extra quotes and everything
fn calculate_etag(data: &[u8]) -> String {
    let digest = md5::compute(data);
    format!("\"{digest:x}\"")
}

/// Get checksum algorithm and value from types like PutObjectInput,
/// that have a field per algorithm (checksum_crc32, checksum_crc64_nvme, etc)
/// Returns Optional<(ChecksumAlgorithm, String)>
#[macro_export]
macro_rules! get_checksum_value {
    ($shape:expr) => {{
        let algorithms_and_optional_values = [
            (ChecksumAlgorithm::Crc32, $shape.checksum_crc32()),
            (ChecksumAlgorithm::Crc32C, $shape.checksum_crc32_c()),
            (ChecksumAlgorithm::Crc64Nvme, $shape.checksum_crc64_nvme()),
            (ChecksumAlgorithm::Sha1, $shape.checksum_sha1()),
            (ChecksumAlgorithm::Sha256, $shape.checksum_sha256()),
        ];

        let mut found: Option<(ChecksumAlgorithm, String)> = None;
        for (algorithm, optional_value) in algorithms_and_optional_values {
            if let Some(value) = optional_value {
                if found.is_some() {
                    panic!("Multiple checksums are set, but only one is allowed.")
                }
                found = Some((algorithm, value.to_string()));
            }
        }

        found
    }};
}

/// Set checksum value on types like PutObjectFluentBuilder,
/// that have a field per algorithm (checksum_crc32, checksum_crc64_nvme, etc).
#[macro_export]
macro_rules! set_checksum_value {
    ($builder:expr, $algorithm:expr, $value:expr) => {
        match $algorithm {
            ChecksumAlgorithm::Crc32 => $builder.checksum_crc32($value),
            ChecksumAlgorithm::Crc32C => $builder.checksum_crc32_c($value),
            ChecksumAlgorithm::Crc64Nvme => $builder.checksum_crc64_nvme($value),
            ChecksumAlgorithm::Sha1 => $builder.checksum_sha1($value),
            ChecksumAlgorithm::Sha256 => $builder.checksum_sha256($value),
            _ => panic!("unknown algorithm"),
        }
    };
}

pin_project! {
    #[derive(Debug)]
    struct TestStream {
        pub parts: Vec<Bytes>,
        next_part_num: u64,
    }
}

impl TestStream {
    pub fn new(parts: Vec<Bytes>) -> Self {
        TestStream {
            parts: parts,
            next_part_num: 1,
        }
    }
}

impl PartStream for TestStream {
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _stream_cx: &aws_s3_transfer_manager::io::StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        let this = self.project();
        let part_index = *this.next_part_num as usize - 1;
        let part = this.parts.get(part_index).map(|b| {
            let part_number = *this.next_part_num;
            *this.next_part_num += 1;
            Ok(PartData::new(part_number, b.clone()))
        });
        Poll::Ready(part)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.parts.iter().map(|b| b.len() as u64).sum())
    }
}

fn mock_s3_client_for_multipart_upload(
    test_strategy: &ChecksumStrategy,
    expected_parts: &Vec<Bytes>,
) -> aws_sdk_s3::Client {
    let mut mock_client_rules: Vec<Rule> = Vec::new();

    // pre-calculate stuff for later
    let upload_id = "test-upload-id".to_string();
    let part_checksums: Vec<String> = expected_parts
        .iter()
        .map(|part| calculate_checksum(&test_strategy.algorithm, part))
        .collect();

    let part_etags: Vec<String> = expected_parts
        .iter()
        .map(|part| calculate_etag(part))
        .collect();

    let multipart_checksum = calculate_multipart_checksum(
        &test_strategy.algorithm,
        &test_strategy.type_if_multipart,
        expected_parts,
    );

    // CreateMultipartUpload
    mock_client_rules.push(
        mock!(aws_sdk_s3::Client::create_multipart_upload)
            .match_requests({
                let test_strategy = test_strategy.clone();
                move |input| {
                    assert_eq!(input.checksum_algorithm(), Some(&test_strategy.algorithm));
                    assert_eq!(
                        input.checksum_type(),
                        Some(&test_strategy.type_if_multipart)
                    );
                    true
                }
            })
            .then_output({
                let upload_id = upload_id.clone();
                let test_strategy = test_strategy.clone();
                move || {
                    CreateMultipartUploadOutput::builder()
                        .upload_id(&upload_id)
                        .checksum_algorithm(test_strategy.algorithm.clone())
                        .checksum_type(test_strategy.type_if_multipart.clone())
                        .build()
                }
            }),
    );

    // N UploadPart rules
    for part_i in 0..expected_parts.len() {
        mock_client_rules.push(
            mock!(aws_sdk_s3::Client::upload_part)
                .match_requests({
                    let upload_id = upload_id.clone();
                    let test_strategy = test_strategy.clone();
                    let part_checksum = part_checksums[part_i].clone();
                    let part_number = part_i as i32 + 1;
                    move |input| {
                        assert_eq!(input.upload_id(), Some(upload_id.as_str()));
                        assert_eq!(input.part_number(), Some(part_number));
                        // If Transfer Manager doesn't know the part's actual checksum value,
                        // it should set the algorithm
                        if let Some((field_algorithm, field_value)) = get_checksum_value!(input) {
                            assert_eq!(field_algorithm, test_strategy.algorithm);
                            assert_eq!(field_value, part_checksum);
                            assert!(input
                                .checksum_algorithm()
                                .is_none_or(|a| a == &test_strategy.algorithm));
                        } else {
                            assert_eq!(input.checksum_algorithm(), Some(&test_strategy.algorithm));
                        }
                        true
                    }
                })
                .then_output({
                    let test_strategy = test_strategy.clone();
                    let part_checksum = part_checksums[part_i].clone();
                    let part_etag = part_etags[part_i].clone();
                    move || {
                        let mut resp = UploadPartOutput::builder().e_tag(&part_etag);
                        resp = set_checksum_value!(resp, test_strategy.algorithm, &part_checksum);
                        resp.build()
                    }
                }),
        );
    }

    // CompleteMultipartUpload
    mock_client_rules.push(
        mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .match_requests({
                let upload_id = upload_id.clone();
                let test_strategy = test_strategy.clone();
                let part_checksums = part_checksums.clone();
                let part_etags = part_etags.clone();
                move |input| {
                    assert_eq!(input.upload_id(), Some(upload_id.as_str()));

                    // if strategy has full-object checksum, that value should be on the CompleteMultipartUpload
                    let input_checksum_field = get_checksum_value!(input);
                    match &test_strategy.full_object_checksum {
                        None => assert!(input_checksum_field.is_none()),
                        Some(full_object_checksum) => {
                            let (field_algorithm, field_value) = input_checksum_field.unwrap();
                            assert_eq!(field_algorithm, test_strategy.algorithm);
                            assert_eq!(&field_value, full_object_checksum);
                        }
                    }

                    let input_parts = input.multipart_upload().unwrap().parts();
                    assert_eq!(input_parts.len(), part_checksums.len());
                    for input_part in input_parts {
                        let part_i =
                            usize::try_from(input_part.part_number().unwrap() - 1).unwrap();
                        assert_eq!(input_part.e_tag(), Some(part_etags[part_i].as_str()));

                        let (input_algorithm, input_checksum) =
                            get_checksum_value!(input_part).unwrap();
                        assert_eq!(input_algorithm, test_strategy.algorithm);
                        assert_eq!(input_checksum, part_checksums[part_i]);
                    }

                    true
                }
            })
            .then_output({
                let test_strategy = test_strategy.clone();
                let multipart_checksum = multipart_checksum.clone();
                move || {
                    let mut req = CompleteMultipartUploadOutput::builder()
                        .checksum_type(test_strategy.type_if_multipart.clone());
                    req = set_checksum_value!(req, &test_strategy.algorithm, &multipart_checksum);
                    req.build()
                }
            }),
    );

    mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::Sequential, &mock_client_rules)
}

// Test user providing a full-object checksum up front, for multipart upload.
#[tokio::test]
async fn test_checksums_full_object_up_front_mpu() {
    let (_guard, _rx) = capture_test_logs();

    let part_size = 5 * MEBIBYTE;

    let parts = vec![
        Bytes::from(vec![b'a'; part_size]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let stream = TestStream::new(parts.clone());
    let checksum_strategy = ChecksumStrategy::with_crc32(calculate_full_object_checksum(
        &ChecksumAlgorithm::Crc32,
        &parts,
    ));

    let client = mock_s3_client_for_multipart_upload(&checksum_strategy, &parts);
    let config = aws_s3_transfer_manager::Config::builder()
        .client(client)
        .part_size(PartSize::Target(part_size as u64))
        .multipart_threshold(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencySetting::Explicit(1)) // guarantee parts sent in order
        .build();
    let tm = aws_s3_transfer_manager::Client::new(config);

    // Do upload
    let upload_handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test_checksums_full_object_up_front_mpu")
        .checksum_strategy(checksum_strategy.clone())
        .body(InputStream::from_part_stream(stream))
        .send()
        .await
        .unwrap();
    let upload_output = upload_handle.join().await.unwrap();

    // Check output
    assert_eq!(
        upload_output.checksum_type,
        Some(checksum_strategy.type_if_multipart)
    );
    let (upload_output_checksum_algorithm, upload_output_checksum_value) =
        get_checksum_value!(upload_output).unwrap();
    assert_eq!(
        upload_output_checksum_algorithm,
        checksum_strategy.algorithm
    );
    if let Some(full_object_checksum) = &checksum_strategy.full_object_checksum {
        assert_eq!(&upload_output_checksum_value, full_object_checksum);
    }
}
