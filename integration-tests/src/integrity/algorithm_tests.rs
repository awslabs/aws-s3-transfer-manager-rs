/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The algorithm matrix: each upload checksum algorithm crossed with each object shape, intact
//! and with its last byte corrupted. Each line names the validation path the shape takes and the
//! outcome for each case.

use crate::algorithm_cases;

algorithm_cases! {
    // 5 MiB single PUT: one GET.
    single_put_one_get {
        object: |alg| Object::single_put(alg).len(5 * MIB);
        targets: [mock_gp, mock_express, real_gp, real_express];
        [crc32, crc32c, crc64nvme, sha1, sha256] via whole_response
            => intact: validated, corrupt: integrity_error;
    }
    // 12 MiB + 5 single PUT: two ranges, no part checksums.
    single_put_split {
        object: |alg| Object::single_put(alg).len(12 * MIB + 5);
        tier: extended;
        targets: [mock_gp, mock_express, real_gp, real_express];
        // ref: RUST-1758
        [crc32, crc32c, crc64nvme] via full_object
            => intact: validated, corrupt: integrity_error;
        [sha1, sha256] via none
            => intact: not_validated(Unavailable), corrupt: undetected(Unavailable);
    }
    // The transfer manager's default multipart upload.
    full_object_mpu {
        object: |alg| Object::mpu(FullObject, alg).len(20 * MIB + 3).parts(8 * MIB);
        tier: extended;
        targets: [mock_gp, real_gp];
        // ref: RUST-1758
        [crc32, crc32c, crc64nvme] via full_object
            => intact: validated, corrupt: integrity_error;
    }
    // Fits in one configured 8 MiB GET, which carries the object's checksum. FULL_OBJECT
    // discovery keeps that size instead of realigning to the 5 MiB stored parts.
    full_object_mpu_small {
        object: |alg| Object::mpu(FullObject, alg).len(7 * MIB).parts(5 * MIB);
        tier: extended;
        targets: [mock_gp, real_gp];
        // ref: RUST-1758
        [crc32, crc32c, crc64nvme] via whole_response
            => intact: validated, corrupt: integrity_error;
    }
    // Stored parts match the download part size.
    composite_aligned {
        object: |alg| Object::mpu(Composite, alg).len(20 * MIB + 3).parts(8 * MIB);
        tier: extended;
        targets: [mock_gp, mock_express, real_gp, real_express];
        [crc32, crc32c, sha1, sha256] via per_part
            => intact: validated, corrupt: integrity_error;
    }
    // Stored parts are 16 MiB; the automatic part size realigns to them.
    composite_realigned {
        object: |alg| Object::mpu(Composite, alg).len(40 * MIB).parts(16 * MIB);
        tier: extended;
        targets: [mock_gp, mock_express, real_gp, real_express];
        [crc32, crc32c, sha1, sha256] via per_part
            => intact: validated, corrupt: integrity_error;
    }
    // An explicit 5 MiB download part size, so ranges don't line up with parts.
    composite_misaligned {
        object: |alg| Object::mpu(Composite, alg).len(20 * MIB + 3).parts(8 * MIB);
        options: Download::new().part_size(5 * MIB);
        tier: extended;
        targets: [mock_gp, mock_express, real_gp, real_express];
        [crc32, crc32c, sha1, sha256] via none
            => intact: not_validated(Unavailable), corrupt: undetected(Unavailable);
    }
    // The client sends no checksum (`RequestChecksumCalculation::WhenRequired`), and S3 stores
    // CRC64NVME FULL_OBJECT anyway, as the mock does. `Object::upload` checks what was stored.
    client_sent_no_checksum {
        object: |_| Object::mpu_without_checksum().len(20 * MIB + 3).parts(8 * MIB);
        tier: extended;
        targets: [mock_gp, mock_express, real_gp, real_express];
        // ref: RUST-1758
        [none] via full_object
            => intact: validated(crc64nvme), corrupt: integrity_error(crc64nvme);
    }
    // Seeded on the mock with no stored checksum, like an object stored before S3 added one by
    // default. A new upload can't produce one, so there are no real targets.
    stored_without_checksum {
        object: |_| Object::seeded_without_checksum().len(20 * MIB + 3);
        tier: extended;
        targets: [mock_gp, mock_express];
        [none] via none
            => intact: not_validated(Unavailable), corrupt: undetected(Unavailable);
    }
    // Directory bucket: FULL_OBJECT parts carry part checksums, so ranges aligned to the parts are
    // validated per part.
    express_full_object_aligned {
        object: |alg| Object::mpu(FullObject, alg).len(20 * MIB + 3).parts(8 * MIB);
        targets: [mock_express, real_express];
        [crc32, crc32c, crc64nvme] via per_part
            => intact: validated, corrupt: integrity_error;
    }
    // Directory bucket, with 7 MiB ranges that don't line up with the 8 MiB parts: no range carries
    // a part checksum.
    express_full_object_misaligned {
        object: |alg| Object::mpu(FullObject, alg).len(20 * MIB + 3).parts(8 * MIB);
        options: Download::new().part_size(7 * MIB);
        targets: [mock_express, real_express];
        // ref: RUST-1758
        [crc32, crc32c, crc64nvme] via full_object
            => intact: validated, corrupt: integrity_error;
    }
}
