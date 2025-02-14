/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{str::FromStr, task::Poll};

use aws_s3_transfer_manager::{
    io::{InputStream, PartData, PartStream, SizeHint},
    metrics::unit::ByteUnit,
    operation::upload::{ChecksumStrategy, UploadOutput},
    types::{ConcurrencyMode, PartSize},
};
use aws_sdk_s3::{
    operation::{
        complete_multipart_upload::CompleteMultipartUploadOutput,
        create_multipart_upload::CreateMultipartUploadOutput, put_object::PutObjectOutput,
        upload_part::UploadPartOutput,
    },
    types::{ChecksumAlgorithm, ChecksumType},
};
use aws_smithy_mocks_experimental::{mock, Rule, RuleMode};
use aws_smithy_runtime::test_util::capture_test_logs::capture_test_logs;
use bytes::Bytes;
use pin_project_lite::pin_project;
use test_common::mock_client_with_stubbed_http_client;

const PART_SIZE: usize = 5 * ByteUnit::Mebibyte.as_bytes_usize();

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
    // S3 doesn't allow CRC64NVME to do composite checksums
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
        parts: Vec<Bytes>,
        next_part_num: u64,
        part_checksums: Vec<Option<String>>,
        full_object_checksum: Option<String>,
    }
}

impl TestStream {
    pub fn new(
        parts: Vec<Bytes>,
        checksum_strategy: Option<ChecksumStrategy>,
        send_checksums_from_part_stream: bool,
        send_full_object_checksum_from_part_stream: bool,
    ) -> Self {
        let part_checksums = if send_checksums_from_part_stream {
            // assert this test is set up in a valid way
            let checksum_strategy = checksum_strategy.as_ref().unwrap();

            parts
                .iter()
                .map(|x| Some(calculate_checksum(checksum_strategy.algorithm(), x)))
                .collect()
        } else {
            vec![None; parts.len()]
        };

        let full_object_checksum = if send_full_object_checksum_from_part_stream {
            // assert this test is set up in a valid way
            let checksum_strategy = checksum_strategy.as_ref().unwrap();
            assert_eq!(
                checksum_strategy.type_if_multipart(),
                &ChecksumType::FullObject
            );
            assert!(checksum_strategy.full_object_checksum().is_none());

            Some(calculate_full_object_checksum(
                checksum_strategy.algorithm(),
                &parts,
            ))
        } else {
            None
        };

        TestStream {
            parts,
            next_part_num: 1,
            part_checksums,
            full_object_checksum,
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
            let mut part_data = PartData::new(part_number, b.clone());
            if let Some(checksum) = &this.part_checksums[part_index] {
                part_data = part_data.with_checksum(checksum);
            }

            Ok(part_data)
        });
        Poll::Ready(part)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.parts.iter().map(|b| b.len() as u64).sum())
    }

    fn full_object_checksum(&self) -> Option<String> {
        self.full_object_checksum.clone()
    }
}

fn mock_s3_client_for_multipart_upload(
    request_strategy: Option<ChecksumStrategy>,
    response_algorithm: ChecksumAlgorithm,
    response_checksum_type: ChecksumType,
    expected_parts: &Vec<Bytes>,
    expect_checksums_from_part_stream: bool,
    expect_full_object_checksum_from_part_stream: bool,
) -> aws_sdk_s3::Client {
    let mut mock_client_rules: Vec<Rule> = Vec::new();

    // Pre-calculate stuff for later
    let upload_id = "test-upload-id".to_string();
    let part_checksums: Vec<String> = expected_parts
        .iter()
        .map(|part| calculate_checksum(&response_algorithm, part))
        .collect();

    let part_etags: Vec<String> = expected_parts
        .iter()
        .map(|part| calculate_etag(part))
        .collect();

    let multipart_checksum =
        calculate_multipart_checksum(&response_algorithm, &response_checksum_type, expected_parts);

    // CreateMultipartUpload
    mock_client_rules.push(
        mock!(aws_sdk_s3::Client::create_multipart_upload)
            .match_requests({
                let request_strategy = request_strategy.clone();
                move |input| {
                    // checksum algorithm and type should only be specified if a strategy is being used
                    assert_eq!(
                        input.checksum_algorithm(),
                        request_strategy.as_ref().map(|s| s.algorithm())
                    );
                    assert_eq!(
                        input.checksum_type(),
                        request_strategy.as_ref().map(|s| s.type_if_multipart())
                    );
                    true
                }
            })
            .then_output({
                let upload_id = upload_id.clone();
                let response_algorithm = response_algorithm.clone();
                let response_checksum_type = response_checksum_type.clone();
                move || {
                    CreateMultipartUploadOutput::builder()
                        .upload_id(&upload_id)
                        .checksum_algorithm(response_algorithm.clone())
                        .checksum_type(response_checksum_type.clone())
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
                    let request_strategy = request_strategy.clone();
                    let part_checksum = part_checksums[part_i].clone();
                    let part_number = part_i as i32 + 1;
                    move |input| {
                        assert_eq!(input.upload_id(), Some(upload_id.as_str()));
                        assert_eq!(input.part_number(), Some(part_number));

                        let mut input_has_checksum_value = false;

                        if let Some(request_strategy) = &request_strategy {
                            // Transfer Manager is doing checksums.
                            // If it doesn't know the part's actual checksum value, it should set the algorithm.
                            if let Some((field_algorithm, field_value)) = get_checksum_value!(input)
                            {
                                input_has_checksum_value = true;

                                assert_eq!(&field_algorithm, request_strategy.algorithm());
                                assert_eq!(field_value, part_checksum);
                                // doesn't matter if algorithm is set too, but if it is, it should be correct
                                if let Some(input_algorithm) = input.checksum_algorithm() {
                                    assert_eq!(input_algorithm, request_strategy.algorithm());
                                }
                            } else {
                                assert!(!expect_checksums_from_part_stream);
                                assert_eq!(
                                    input.checksum_algorithm(),
                                    Some(request_strategy.algorithm())
                                );
                            }
                        } else {
                            // Transfer Manager is not doing checksums
                            assert_eq!(input.checksum_algorithm(), None);
                            assert!(get_checksum_value!(input).is_none());
                        }

                        // Assert that, if test is configured to stream part checksums, that they're in the request
                        assert_eq!(input_has_checksum_value, expect_checksums_from_part_stream);
                        true
                    }
                })
                .then_output({
                    let response_algorithm = response_algorithm.clone();
                    let part_checksum = part_checksums[part_i].clone();
                    let part_etag = part_etags[part_i].clone();
                    move || {
                        let mut resp = UploadPartOutput::builder().e_tag(&part_etag);
                        // As of 2025, S3 always sends checksums in UploadPart responses
                        resp = set_checksum_value!(resp, response_algorithm, &part_checksum);
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
                let request_strategy = request_strategy.clone();
                let response_algorithm = response_algorithm.clone();
                let part_checksums = part_checksums.clone();
                let part_etags = part_etags.clone();
                let multipart_checksum = multipart_checksum.clone();
                move |input| {
                    assert_eq!(input.upload_id(), Some(upload_id.as_str()));

                    // The full-object checksum value should be in the CompleteMultipartUpload
                    // if it was passed via ChecksumStrategy, or via PartStream.
                    let input_checksum_field = get_checksum_value!(input);
                    if let Some(request_strategy) = &request_strategy {
                        assert_eq!(
                            input.checksum_type(),
                            Some(request_strategy.type_if_multipart())
                        );

                        if let Some((field_algorithm, field_value)) = &input_checksum_field {
                            assert_eq!(field_algorithm, request_strategy.algorithm());
                            assert_eq!(field_value, &multipart_checksum);
                            assert!(
                                request_strategy.full_object_checksum().is_some()
                                    || expect_full_object_checksum_from_part_stream
                            );
                        } else {
                            assert!(request_strategy.full_object_checksum().is_none());
                            assert!(!expect_full_object_checksum_from_part_stream);
                        }
                    } else {
                        assert!(input_checksum_field.is_none());
                        assert!(input.checksum_type.is_none());
                    }

                    // The multipart_upload struct should include info about each part
                    let input_parts = input.multipart_upload().unwrap().parts();
                    assert_eq!(input_parts.len(), part_checksums.len());
                    for input_part in input_parts {
                        let part_i =
                            usize::try_from(input_part.part_number().unwrap() - 1).unwrap();
                        assert_eq!(input_part.e_tag(), Some(part_etags[part_i].as_str()));

                        // As of 2025, S3 always sends a checksum in UploadPart responses,
                        // ensure these checksums are reported in CompleteMultipartUpload.
                        // (Technically, it would be OK to omit the part checksums in a few situations,
                        // they only seem to be required for composite checksums,
                        // but it never hurts to include them, so do it for simplicity's sake)
                        let (input_part_algorithm, input_part_checksum) =
                            get_checksum_value!(input_part).unwrap();
                        assert_eq!(input_part_checksum, part_checksums[part_i]);
                        assert_eq!(input_part_algorithm, response_algorithm);
                    }

                    true
                }
            })
            .then_output({
                let response_algorithm = response_algorithm.clone();
                let response_checksum_type = response_checksum_type.clone();
                let multipart_checksum = multipart_checksum.clone();
                move || {
                    // As of 2025, S3 always sends a checksum in CompleteMultipartUpload response
                    let mut req = CompleteMultipartUploadOutput::builder()
                        .checksum_type(response_checksum_type.clone());
                    req = set_checksum_value!(req, &response_algorithm, &multipart_checksum);
                    req.build()
                }
            }),
    );

    mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::Sequential, &mock_client_rules)
}

fn mock_s3_client_for_put_object(
    request_strategy: Option<ChecksumStrategy>,
    response_algorithm: ChecksumAlgorithm,
    expected_body: &Bytes,
) -> aws_sdk_s3::Client {
    let mut mock_client_rules: Vec<Rule> = Vec::new();

    // Pre-calculate stuff for later
    let checksum_value = calculate_checksum(&response_algorithm, expected_body);
    let etag = calculate_etag(expected_body);

    // PutObject
    mock_client_rules.push(
        mock!(aws_sdk_s3::Client::put_object)
            .match_requests({
                let request_strategy = request_strategy.clone();
                move |input| {
                    if let Some(request_strategy) = &request_strategy {
                        // Transfer Manager is doing checksums.
                        if let Some(provided_checksum) = &request_strategy.full_object_checksum() {
                            // Full-object checksum provided up front. Assert it was used
                            let (input_checksum_algorithm, input_checksum_value) =
                                get_checksum_value!(input).unwrap();
                            assert_eq!(&input_checksum_algorithm, request_strategy.algorithm());
                            assert_eq!(&input_checksum_value, provided_checksum);
                        } else {
                            // Transfer Manager should set algorithm, so SDK will calculate actual checksum value
                            assert_eq!(
                                input.checksum_algorithm(),
                                Some(request_strategy.algorithm())
                            );
                        }
                    } else {
                        // Transfer Manager is not doing checksums
                        assert!(get_checksum_value!(input).is_none());
                        assert_eq!(input.checksum_algorithm(), None);
                    }

                    true
                }
            })
            .then_output({
                let response_algorithm = response_algorithm.clone();
                let checksum_value = checksum_value.clone();
                let etag = etag.clone();
                move || {
                    // As of 2025, S3 always sends checksums in PutObject responses
                    let mut resp = PutObjectOutput::builder()
                        .e_tag(&etag)
                        .checksum_type(ChecksumType::FullObject);
                    resp = set_checksum_value!(resp, response_algorithm, &checksum_value);

                    resp.build()
                }
            }),
    );

    mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::Sequential, &mock_client_rules)
}

async fn test_mpu(
    user_checksum_strategy: Option<ChecksumStrategy>,
    parts: Vec<Bytes>,
) -> UploadOutput {
    run_test(TestConfig {
        user_checksum_strategy,
        parts,
        send_as_multipart: true,
        ..Default::default()
    })
    .await
}

async fn test_put_object(
    user_checksum_strategy: Option<ChecksumStrategy>,
    body: Bytes,
) -> UploadOutput {
    run_test(TestConfig {
        user_checksum_strategy,
        parts: vec![body],
        send_as_multipart: false,
        ..Default::default()
    })
    .await
}

struct TestConfig {
    user_checksum_strategy: Option<ChecksumStrategy>,
    parts: Vec<Bytes>,
    send_as_multipart: bool,
    send_checksums_from_part_stream: bool,
    send_full_object_checksum_from_part_stream: bool,
    sdk_checksum_calculation: aws_sdk_s3::config::RequestChecksumCalculation,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            user_checksum_strategy: None,
            parts: Vec::new(),
            send_as_multipart: true,
            send_checksums_from_part_stream: false,
            send_full_object_checksum_from_part_stream: false,
            sdk_checksum_calculation: aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported,
        }
    }
}

impl TestConfig {
    async fn run(self) -> UploadOutput {
        run_test(self).await
    }
}

async fn run_test(config: TestConfig) -> UploadOutput {
    let (_guard, _rx) = capture_test_logs();

    // This is the ChecksumStrategy we expect the Transfer Manager to use while sending requests
    let request_checksum_strategy = config.user_checksum_strategy.clone().or(
        // If user didn't set a strategy, Transfer Manager should fall back to default strategy,
        // unless user also disabled checksums via SDK's config.
        if config.sdk_checksum_calculation
            == aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported
        {
            Some(ChecksumStrategy::default())
        } else {
            None
        },
    );

    // Determine which kind of checksums will be in the response.
    // As of 2025, S3 always provides checksums in response, even if request had none.
    let (response_checksum_algorithm, response_checksum_type) = match &request_checksum_strategy {
        None => (ChecksumAlgorithm::Crc64Nvme, ChecksumType::FullObject),
        Some(request_checksum_strategy) => (
            request_checksum_strategy.algorithm().clone(),
            request_checksum_strategy.type_if_multipart().clone(),
        ),
    };

    // Create mock aws_sdk_s3::Client
    let s3_client = if config.send_as_multipart {
        mock_s3_client_for_multipart_upload(
            request_checksum_strategy.clone(),
            response_checksum_algorithm.clone(),
            response_checksum_type.clone(),
            &config.parts,
            config.send_checksums_from_part_stream,
            config.send_full_object_checksum_from_part_stream,
        )
    } else {
        assert_eq!(config.parts.len(), 1);
        mock_s3_client_for_put_object(
            request_checksum_strategy.clone(),
            response_checksum_algorithm.clone(),
            &config.parts[0],
        )
    };

    // Adjust client's SDK config to use specific RequestChecksumCalculation
    let s3_client = aws_sdk_s3::Client::from_conf(
        s3_client
            .config()
            .to_builder()
            .request_checksum_calculation(config.sdk_checksum_calculation)
            .build(),
    );

    let body_stream: InputStream = if config.send_as_multipart {
        InputStream::from_part_stream(TestStream::new(
            config.parts.clone(),
            request_checksum_strategy.clone(),
            config.send_checksums_from_part_stream,
            config.send_full_object_checksum_from_part_stream,
        ))
    } else {
        assert!(!config.send_checksums_from_part_stream);
        assert!(!config.send_full_object_checksum_from_part_stream);
        config.parts[0].clone().into()
    };

    let tm_config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .part_size(PartSize::Target(PART_SIZE as u64))
        .multipart_threshold(PartSize::Target(PART_SIZE as u64))
        .concurrency(ConcurrencyMode::Explicit(1)) // guarantee parts sent in order
        .build();
    let tm = aws_s3_transfer_manager::Client::new(tm_config);

    // Do upload
    let upload_handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .set_checksum_strategy(config.user_checksum_strategy.clone())
        .body(body_stream)
        .initiate()
        .unwrap();
    let upload_output = upload_handle.join().await.unwrap();

    // As of 2025, S3 always sends a checksum in the response.
    // Assert that checksum type and value fields are set.
    // Don't check exact contents though, leave that up to individual tests.
    assert!(upload_output.checksum_type().is_some());
    assert!(get_checksum_value!(upload_output).is_some());

    upload_output
}

//
// Multipart Upload Tests
//

#[tokio::test]
async fn test_mpu_provided_full_object_crc32() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let full_object_checksum = calculate_full_object_checksum(&ChecksumAlgorithm::Crc32, &parts);
    let strategy = ChecksumStrategy::with_crc32(&full_object_checksum);
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(output.checksum_crc32(), Some(full_object_checksum.as_ref()));
}

#[tokio::test]
async fn test_mpu_provided_full_object_crc32_c() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let full_object_checksum = calculate_full_object_checksum(&ChecksumAlgorithm::Crc32C, &parts);
    let strategy = ChecksumStrategy::with_crc32_c(&full_object_checksum);
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(
        output.checksum_crc32_c(),
        Some(full_object_checksum.as_ref())
    );
}

#[tokio::test]
async fn test_mpu_provided_full_object_crc64_nvme() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let full_object_checksum =
        calculate_full_object_checksum(&ChecksumAlgorithm::Crc64Nvme, &parts);
    let strategy = ChecksumStrategy::with_crc64_nvme(&full_object_checksum);
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(
        output.checksum_crc64_nvme(),
        Some(full_object_checksum.as_ref())
    );
}

// NOTE: SHA algorithms not currently allowed to provide full-object checksums,
// because it would prevent Transfer Manager from doing multipart upload.

#[tokio::test]
async fn test_mpu_calculated_full_object_crc32() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_crc32();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32().is_some());
}

#[tokio::test]
async fn test_mpu_calculated_full_object_crc32_c() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_crc32_c();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32_c().is_some());
}

#[tokio::test]
async fn test_mpu_calculated_full_object_crc64_nvme() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_crc64_nvme();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

// NOTE: SHA full-object MPU checksums not supported by S3

#[tokio::test]
async fn test_mpu_calculated_composite_crc32() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_crc32_composite_if_multipart();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::Composite));
    assert!(output.checksum_crc32().is_some());
}

#[tokio::test]
async fn test_mpu_calculated_composite_crc32_c() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_crc32_c_composite_if_multipart();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::Composite));
    assert!(output.checksum_crc32_c().is_some());
}

// NOTE: CRC64NVME composite checksums not supported by S3

#[tokio::test]
async fn test_mpu_calculated_composite_sha1() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_sha1_composite_if_multipart();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::Composite));
    assert!(output.checksum_sha1().is_some());
}

#[tokio::test]
async fn test_mpu_calculated_composite_sha256() {
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy = ChecksumStrategy::with_calculated_sha256_composite_if_multipart();
    let output = test_mpu(Some(strategy), parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::Composite));
    assert!(output.checksum_sha256().is_some());
}
#[tokio::test]
async fn test_mpu_default_strategy() {
    // Test where user didn't set a strategy, but SDK is calculating checksums wherever possible (its default behavior).
    // Transfer Manager should end up using the default checksum strategy (full-object CRC64NVME).
    let parts = vec![
        Bytes::from(vec![b'a'; PART_SIZE]),
        Bytes::from("abcdefghijklm".as_bytes()),
    ];
    let strategy: Option<ChecksumStrategy> = None;
    let output = test_mpu(strategy, parts).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

#[tokio::test]
async fn test_mpu_no_strategy() {
    // Test where user didn't set a strategy AND disabled checksums via SDK's config.
    // Transfer Manager should not send any checksum data. But it will still receive checksums because,
    // as of 2025, S3 always sends them in responses (defaulting to full-object CRC64NVME).
    let output = TestConfig {
        parts: vec![
            Bytes::from(vec![b'a'; PART_SIZE]),
            Bytes::from("abcdefghijklm".as_bytes()),
        ],
        user_checksum_strategy: None,
        send_as_multipart: true,
        sdk_checksum_calculation: aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
        ..Default::default()
    }
    .run()
    .await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

//
// PutObject Upload Tests
//

#[tokio::test]
async fn test_put_object_provided_full_object_crc32() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let full_object_checksum = calculate_checksum(&ChecksumAlgorithm::Crc32, &body);
    let strategy = ChecksumStrategy::with_crc32(&full_object_checksum);
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(output.checksum_crc32(), Some(full_object_checksum.as_ref()));
}

#[tokio::test]
async fn test_put_object_provided_full_object_crc32_c() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let full_object_checksum = calculate_checksum(&ChecksumAlgorithm::Crc32C, &body);
    let strategy = ChecksumStrategy::with_crc32_c(&full_object_checksum);
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(
        output.checksum_crc32_c(),
        Some(full_object_checksum.as_ref())
    );
}

#[tokio::test]
async fn test_put_object_provided_full_object_crc64_nvme() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let full_object_checksum = calculate_checksum(&ChecksumAlgorithm::Crc64Nvme, &body);
    let strategy = ChecksumStrategy::with_crc64_nvme(&full_object_checksum);
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(
        output.checksum_crc64_nvme(),
        Some(full_object_checksum.as_ref())
    );
}

// NOTE: SHA algorithms not currently allowed to provide full-object checksums,
// because it would prevent Transfer Manager from doing multipart upload.

#[tokio::test]
async fn test_put_object_calculated_full_object_crc32() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_crc32();
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32().is_some());
}

#[tokio::test]
async fn test_put_object_calculated_full_object_crc32_c() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_crc32_c();
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32_c().is_some());
}

#[tokio::test]
async fn test_put_object_calculated_full_object_crc64_nvme() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_crc64_nvme();
    let output = test_put_object(Some(strategy), body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

#[tokio::test]
async fn test_put_object_calculated_crc32_composite_if_multipart() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_crc32_composite_if_multipart();
    let output = test_put_object(Some(strategy), body).await;
    // NOTE: since it wasn't multipart, the checksum isn't composite
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32().is_some());
}

#[tokio::test]
async fn test_put_object_calculated_crc32_c_composite_if_multipart() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_crc32_c_composite_if_multipart();
    let output = test_put_object(Some(strategy), body).await;
    // NOTE: since it wasn't multipart, the checksum isn't composite
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32_c().is_some());
}

// NOTE: CRC64NVME composite checksums not supported by S3

#[tokio::test]
async fn test_put_object_calculated_sha1_composite_if_multipart() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_sha1_composite_if_multipart();
    let output = test_put_object(Some(strategy), body).await;
    // NOTE: since it wasn't multipart, the checksum isn't composite
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_sha1().is_some());
}

#[tokio::test]
async fn test_put_object_calculated_sha256_composite_if_multipart() {
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy = ChecksumStrategy::with_calculated_sha256_composite_if_multipart();
    let output = test_put_object(Some(strategy), body).await;
    // NOTE: since it wasn't multipart, the checksum isn't composite
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_sha256().is_some());
}
#[tokio::test]
async fn test_put_object_default_strategy() {
    // Test where user didn't set a strategy, but SDK is calculating checksums wherever possible (its default behavior).
    // Transfer Manager should end up using the default checksum strategy (full-object CRC64NVME).
    let body = Bytes::from("abcdefghijklm".as_bytes());
    let strategy: Option<ChecksumStrategy> = None;
    let output = test_put_object(strategy, body).await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

#[tokio::test]
async fn test_put_object_no_strategy() {
    // Test where user didn't set a strategy AND disabled checksums via SDK's config.
    // Transfer Manager should not send any checksum data. But it will still receive checksums because,
    // as of 2025, S3 always sends them in responses (defaulting to full-object CRC64NVME).
    let output = TestConfig {
        parts: vec![Bytes::from("abcdefghijklm".as_bytes())],
        user_checksum_strategy: None,
        send_as_multipart: false,
        sdk_checksum_calculation: aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
        ..Default::default()
    }
    .run()
    .await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc64_nvme().is_some());
}

#[tokio::test]
async fn test_mpu_checksums_from_part_stream() {
    // Test where the PartStream provides checksum values, instead of letting the SDK calculate them.
    let output = TestConfig {
        parts: vec![
            Bytes::from(vec![b'a'; PART_SIZE]),
            Bytes::from("abcdefghijklm".as_bytes()),
        ],
        user_checksum_strategy: Some(ChecksumStrategy::with_calculated_crc32()),
        send_as_multipart: true,
        send_checksums_from_part_stream: true,
        send_full_object_checksum_from_part_stream: true,
        ..Default::default()
    }
    .run()
    .await;
    assert_eq!(output.checksum_type(), Some(&ChecksumType::FullObject));
    assert!(output.checksum_crc32().is_some());
}
