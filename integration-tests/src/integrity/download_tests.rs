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
        // The automatic part size: discovery fetches range 0, discards it, and realigns with a
        // `partNumber=1` request. Faults stay off range 0, which is fetched twice.
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
            ignore: "not validated against the full-object checksum";
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
            ignore: "not validated against the full-object checksum";
            expect validated { [stream] }
            expect integrity_error { corrupt(later): [path] }
        }
        // 7 MiB ranges start neither at part boundaries nor at powers of two.
        download odd_part_size {
            options: Download::new().part_size(7 * MIB);
            // ref: RUST-1758
            ignore: "not validated against the full-object checksum";
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

    // 65 ranges with at most 8 requests in flight: several waves of requests.
    full_object_large {
        object: |alg| Object::mpu(FullObject, alg).len(64 * 8 * MIB + 17).parts(8 * MIB);
        algorithms: [crc64nvme];
        via: full_object;
        targets: [mock_gp];

        download out_of_order {
            options: Download::new().part_size(8 * MIB).concurrency(8);
            hold: first_range;
            // ref: RUST-1758
            ignore: "not validated against the full-object checksum";
            expect validated { [path] }
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
            ignore: "not validated against the full-object checksum";
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
