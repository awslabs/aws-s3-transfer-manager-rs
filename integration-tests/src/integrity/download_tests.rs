/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Downloads of each kind of object: faults, sinks and download options, per validation path.

use crate::download_cases;

download_cases! {
    // Below the multipart threshold: one PUT, and the download fits in one GET.
    single_put {
        object: |alg| Object::single_put(alg).len(5 * MIB);
        algorithms: [crc64nvme, crc32];          // the TM default, and the SDK's
        via: whole_response;
        targets: [mock_gp];

        download default {
            options: Download::new();
            expect validated {
                [stream, path, file],
                truncate(first): [stream],
            }
            expect integrity_error {
                corrupt(first): [stream, path, file],
            }
        }
        // With validation off, a corrupted byte is delivered.
        download validation_off {
            options: Download::new().validation_off();
            expect not_validated(Disabled) { [stream] }
            expect undetected(Disabled) { corrupt(first): [stream] }
        }
    }

    // A COMPOSITE multipart upload in 8 MiB parts. A response whose range matches a part carries
    // that part's checksum.
    composite_mpu {
        object: |alg| Object::mpu(Composite, alg).len(20 * MIB + 3).parts(8 * MIB);
        algorithms: [crc32, sha256];             // a CRC, and a hash that can't be combined
        via: per_part;
        targets: [mock_gp];

        // An explicit part size equal to the stored one: ranges line up with parts, and the first
        // GET is the first range.
        download aligned {
            options: Download::new().part_size(8 * MIB);
            expect validated {
                [stream, path, file],
                truncate(first): [stream],
                truncate(later): [stream],
            }
            expect integrity_error {
                corrupt(first): [stream],
                corrupt(later): [path, file],
                corrupt_retry(later): [stream],
            }
        }
        // HEAD resolves the object metadata. A `partNumber=1` GET supplies the first body
        // and the stored part size used for later ranges.
        download auto_part_size {
            options: Download::new();
            expect validated { [stream], truncate(later): [stream] }
            expect integrity_error { corrupt(later): [stream] }
        }
        download out_of_order {
            options: Download::new().part_size(8 * MIB);
            hold: first_range;
            expect validated { [stream] }
            expect integrity_error { corrupt(later): [path] }
        }
    }

    // The TM's default multipart upload: FULL_OBJECT, so ranges carry no checksum.
    full_object_mpu {
        object: |alg| Object::mpu(FullObject, alg).len(20 * MIB + 3).parts(8 * MIB);
        algorithms: [crc64nvme, crc32];          // the TM default, and a 32-bit CRC
        via: full_object;
        targets: [mock_gp, real_gp];

        // The transfer manager's defaults: what most callers get.
        download default {
            options: Download::new();
            // ref: RUST-1758
            expect validated {
                [stream, path, file],
                truncate(first): [stream],
                truncate(later): [stream],
            }
            expect integrity_error {
                corrupt(first): [stream],
                corrupt(later): [path, file],
                corrupt(tail): [stream],
                corrupt_retry(later): [stream],
            }
        }
        download out_of_order {
            options: Download::new().part_size(8 * MIB);
            hold: first_range;
            // ref: RUST-1758
            expect validated { [stream] }
            expect integrity_error { corrupt(later): [path] }
        }
        // 7 MiB ranges start neither at part boundaries nor at powers of two.
        download odd_part_size {
            options: Download::new().part_size(7 * MIB);
            // ref: RUST-1758
            expect validated { [path] }
            expect integrity_error { corrupt(later): [stream] }
        }
        // A caller range that starts and ends off part and range boundaries, and spans the 8 MiB
        // boundary between parts 1 and 2.
        download caller_range {
            options: Download::new().range(1_000_003..=15_000_006);
            expect not_validated(Unavailable) { [stream] }
        }
        download validation_off {
            options: Download::new().validation_off();
            expect not_validated(Disabled) { [stream] }
            expect undetected(Disabled) { corrupt(later): [stream] }
        }
    }

    // Five 5 MiB ranges with at most 2 requests in flight: the other ranges go out in several
    // waves while the first is held.
    full_object_waves {
        object: |alg| Object::mpu(FullObject, alg).len(20 * MIB + 3).parts(8 * MIB);
        algorithms: [crc64nvme];
        via: full_object;
        targets: [mock_gp];

        download out_of_order {
            options: Download::new().part_size(5 * MIB).concurrency(2);
            hold: first_range;
            // ref: RUST-1758
            expect validated { [path] }
        }
    }

    // 12 MiB + 5 single PUT, downloaded in two ranges: neither carries a checksum, so a file sink
    // is published or kept only by the full-object check.
    single_put_split {
        object: |alg| Object::single_put(alg).len(12 * MIB + 5);
        algorithms: [crc64nvme];
        via: full_object;
        targets: [mock_gp];

        download default {
            options: Download::new();
            // ref: RUST-1758
            expect validated { [path, file] }
            expect integrity_error { corrupt(later): [path, file] }
        }
    }

    // Parts of 8, 5 and 7 MiB. The automatic part size realigns to part 1's 8 MiB, so only the
    // first range matches a part: it's validated, and the rest aren't.
    unequal_parts {
        object: |alg| Object::mpu_parts(Composite, alg, &[8 * MIB, 5 * MIB, 7 * MIB]);
        algorithms: [crc32];
        via: per_part;
        targets: [mock_gp, real_gp];

        download default {
            options: Download::new();
            expect not_validated(PartialCoverage) { [stream] }
            expect integrity_error { corrupt(first): [stream] }
            expect undetected(PartialCoverage) { corrupt(later): [stream] }
        }
    }

    // 7 MiB COMPOSITE in 5 MiB parts, fetched in one 8 MiB GET. The response carries the
    // composite `-2` value, which the SDK can't validate.
    composite_one_get {
        object: |alg| Object::mpu(Composite, alg).len(7 * MIB).parts(5 * MIB);
        algorithms: [crc32];
        via: none;
        targets: [mock_gp, real_gp];

        download default {
            options: Download::new().part_size(8 * MIB);
            expect not_validated(CompositeChecksum) { [stream] }
            expect undetected(CompositeChecksum) { corrupt(first): [stream] }
        }
    }

    // An empty object with a CRC: validated against the CRC of empty input.
    empty_crc {
        object: |alg| Object::empty(alg);
        algorithms: [crc64nvme];
        via: full_object;
        targets: [mock_gp];

        download default {
            options: Download::new();
            // ref: RUST-1758
            expect validated { [stream, path] }
        }
    }

    // An empty object with a SHA checksum: a download that delivers no bytes reports `Unavailable`,
    // and the full-object check applies only to CRCs.
    empty_sha {
        object: |alg| Object::empty(alg);
        algorithms: [sha256];
        via: none;
        targets: [mock_gp];

        download default {
            options: Download::new();
            expect not_validated(Unavailable) { [stream, path] }
        }
    }
}

/// HEAD-first discovery must retain the documented request costs and validation
/// outcomes. COMPOSITE discovery keeps the layout response as the first body;
/// caller ranges and validation-off downloads keep GET-first discovery.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_request_counts_and_verdicts() {
    use super::test_util::vocab::CaseTarget;
    use s3_mock_server::Operation;

    let cases = super::table_cases();
    for (suffix, heads, gets) in [
        (
            "download_tests::single_put::crc32::default::validated::stream",
            1,
            1,
        ),
        (
            "algorithm_tests::full_object_mpu_small::extended::crc32::validated::stream",
            1,
            1,
        ),
        (
            "download_tests::full_object_mpu::crc64nvme::default::validated::stream",
            1,
            3,
        ),
        (
            "algorithm_tests::single_put_split::extended::crc32::validated::stream",
            1,
            2,
        ),
        (
            "algorithm_tests::single_put_split::extended::sha256::not_validated::stream",
            1,
            2,
        ),
        (
            "download_tests::composite_mpu::crc32::auto_part_size::validated::stream",
            1,
            3,
        ),
        (
            "algorithm_tests::composite_realigned::extended::crc32::validated::stream",
            1,
            3,
        ),
        (
            "download_tests::composite_mpu::crc32::aligned::validated::stream",
            1,
            3,
        ),
        (
            "download_tests::empty_crc::crc64nvme::default::validated::stream",
            1,
            0,
        ),
        (
            "algorithm_tests::stored_without_checksum::extended::none::not_validated::stream",
            1,
            3,
        ),
        (
            "download_tests::full_object_mpu::crc32::caller_range::not_validated::stream",
            0,
            2,
        ),
        (
            "download_tests::single_put::crc32::validation_off::not_validated::stream",
            0,
            1,
        ),
    ] {
        let case = cases
            .iter()
            .find(|case| case.path.ends_with(suffix))
            .unwrap_or_else(|| panic!("no case {suffix}"));
        let execution = case.execute(CaseTarget::MockGp).await;
        case.check(&execution)
            .unwrap_or_else(|error| panic!("{suffix}: {error}"));
        let requests: Vec<_> = execution
            .requests
            .as_ref()
            .expect("mock journal")
            .iter()
            .filter(|request| request.key.as_deref() == Some(execution.key.as_str()))
            .collect();
        let count = |operation| {
            requests
                .iter()
                .filter(|request| request.operation == operation)
                .count()
        };
        assert_eq!(
            (count(Operation::HeadObject), count(Operation::GetObject)),
            (heads, gets),
            "{suffix}: {requests:#?}",
        );
        let first = if heads == 0 {
            Operation::GetObject
        } else {
            Operation::HeadObject
        };
        assert_eq!(requests[0].operation, first, "{suffix}: {requests:#?}");
    }
}
