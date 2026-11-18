/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`Object`]: a description of an object to upload, which uploads it and checks what was stored.

use std::pin::Pin;
use std::task::{Context, Poll};

use aws_sdk_s3::types::{ChecksumMode, ChecksumType};
use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::types::checksum::{Crc32, Crc32C, Crc64Nvme, Sha1, Sha256};
use aws_sdk_s3_transfer_manager::types::ChecksumStrategy;

use crate::assertions::{describe_error, describe_sdk_error};
use crate::harness::{ClientOptions, TmTestClient};
use crate::test_util::data::{fill_object_bytes, object_bytes};

/// One mebibyte.
pub(crate) const MIB: u64 = 1 << 20;

/// Objects larger than this are uploaded from a temporary file rather than from memory.
const IN_MEMORY_UPLOAD_LIMIT: u64 = 64 * MIB;

/// An upload checksum algorithm, or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Alg {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
    /// No checksum: the client sends none, or the object is stored without one.
    NoChecksum,
}

impl Alg {
    /// Every algorithm but [`Alg::NoChecksum`], in the order S3 lists them.
    pub(crate) const WITH_CHECKSUM: [Alg; 5] = [
        Alg::Crc32,
        Alg::Crc32c,
        Alg::Crc64Nvme,
        Alg::Sha1,
        Alg::Sha256,
    ];

    /// The SDK's `ChecksumAlgorithm`. `None` for [`Alg::NoChecksum`].
    pub(crate) fn sdk(self) -> Option<aws_sdk_s3::types::ChecksumAlgorithm> {
        use aws_sdk_s3::types::ChecksumAlgorithm as A;
        Some(match self {
            Alg::Crc32 => A::Crc32,
            Alg::Crc32c => A::Crc32C,
            Alg::Crc64Nvme => A::Crc64Nvme,
            Alg::Sha1 => A::Sha1,
            Alg::Sha256 => A::Sha256,
            Alg::NoChecksum => return None,
        })
    }

    /// The algorithm as the transfer manager names it. `None` for [`Alg::NoChecksum`].
    pub(crate) fn tm(self) -> Option<aws_sdk_s3_transfer_manager::types::ChecksumAlgorithm> {
        use aws_sdk_s3_transfer_manager::types::ChecksumAlgorithm as A;
        Some(match self {
            Alg::Crc32 => A::Crc32,
            Alg::Crc32c => A::Crc32C,
            Alg::Crc64Nvme => A::Crc64Nvme,
            Alg::Sha1 => A::Sha1,
            Alg::Sha256 => A::Sha256,
            Alg::NoChecksum => return None,
        })
    }

    fn hasher(self) -> Option<Box<dyn aws_smithy_checksums::Checksum>> {
        use aws_smithy_checksums::ChecksumAlgorithm as A;
        Some(
            match self {
                Alg::Crc32 => A::Crc32,
                Alg::Crc32c => A::Crc32c,
                Alg::Crc64Nvme => A::Crc64Nvme,
                Alg::Sha1 => A::Sha1,
                Alg::Sha256 => A::Sha256,
                Alg::NoChecksum => return None,
            }
            .into_impl(),
        )
    }
}

/// How an object is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Layout {
    /// One PutObject.
    SinglePut,
    /// A multipart upload with a FULL_OBJECT checksum.
    FullObject,
    /// A multipart upload with a COMPOSITE checksum.
    Composite,
    /// One PutObject of no bytes.
    Empty,
}

/// What to upload. The builders are `const` so an object can be part of a `static`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Object {
    pub(crate) layout: Layout,
    pub(crate) alg: Alg,
    pub(crate) len: u64,
    /// The size of each part but the last. 0 for a single PUT.
    pub(crate) part_size: u64,
    /// Unequal part sizes, in order. Empty for equal parts of `part_size`.
    pub(crate) part_sizes: &'static [u64],
    /// Stored directly in the mock's storage instead of uploaded. Mock targets only.
    pub(crate) seeded: bool,
}

impl Object {
    const fn base(layout: Layout, alg: Alg, len: u64, part_size: u64) -> Self {
        Object {
            layout,
            alg,
            len,
            part_size,
            part_sizes: &[],
            seeded: false,
        }
    }

    /// A single PUT, 5 MiB unless [`len`](Self::len) changes it.
    pub(crate) const fn single_put(alg: Alg) -> Self {
        Self::base(Layout::SinglePut, alg, 5 * MIB, 0)
    }

    /// A multipart upload, 20 MiB + 3 in 8 MiB parts unless [`len`](Self::len) and
    /// [`parts`](Self::parts) change it.
    pub(crate) const fn mpu(layout: Layout, alg: Alg) -> Self {
        Self::base(layout, alg, 20 * MIB + 3, 8 * MIB)
    }

    /// A multipart upload with these part sizes, in order. Panics if `part_sizes` is empty.
    pub(crate) const fn mpu_parts(layout: Layout, alg: Alg, part_sizes: &'static [u64]) -> Self {
        let mut len = 0;
        let mut i = 0;
        while i < part_sizes.len() {
            len += part_sizes[i];
            i += 1;
        }
        Object {
            layout,
            alg,
            len,
            part_size: part_sizes[0],
            part_sizes,
            seeded: false,
        }
    }

    /// A multipart upload from a client that sends no checksum
    /// (`RequestChecksumCalculation::WhenRequired`). S3 stores a CRC64NVME FULL_OBJECT checksum.
    pub(crate) const fn mpu_without_checksum() -> Self {
        Self::base(Layout::FullObject, Alg::NoChecksum, 20 * MIB + 3, 8 * MIB)
    }

    /// An object stored in the mock with no checksum at all, as objects stored before S3 added
    /// one by default.
    pub(crate) const fn seeded_without_checksum() -> Self {
        Object {
            seeded: true,
            ..Self::base(Layout::SinglePut, Alg::NoChecksum, 20 * MIB + 3, 0)
        }
    }

    /// A single PUT of no bytes.
    pub(crate) const fn empty(alg: Alg) -> Self {
        Self::base(Layout::Empty, alg, 0, 0)
    }

    /// Sets the length in bytes. Not for an object from [`mpu_parts`](Self::mpu_parts), whose
    /// length is the sum of its parts.
    pub(crate) const fn len(mut self, len: u64) -> Self {
        self.len = len;
        self
    }

    /// Sets the size of each stored part but the last. Ignored for a single PUT.
    pub(crate) const fn parts(mut self, part_size: u64) -> Self {
        self.part_size = part_size;
        self
    }

    fn is_multipart(&self) -> bool {
        matches!(self.layout, Layout::FullObject | Layout::Composite)
    }

    /// The size of each stored part, in order. Empty for a single PUT.
    pub(crate) fn stored_part_sizes(&self) -> Vec<u64> {
        if !self.part_sizes.is_empty() {
            return self.part_sizes.to_vec();
        }
        if !self.is_multipart() {
            return Vec::new();
        }
        let mut sizes = vec![self.part_size; (self.len / self.part_size) as usize];
        if !self.len.is_multiple_of(self.part_size) {
            sizes.push(self.len % self.part_size);
        }
        sizes
    }

    fn strategy(&self) -> Option<ChecksumStrategy> {
        Some(match (self.layout, self.alg) {
            (_, Alg::NoChecksum) => return None,
            (Layout::Composite, Alg::Crc32) => ChecksumStrategy::composite(Crc32),
            (Layout::Composite, Alg::Crc32c) => ChecksumStrategy::composite(Crc32C),
            (_, Alg::Crc32) => ChecksumStrategy::full_object(Crc32),
            (_, Alg::Crc32c) => ChecksumStrategy::full_object(Crc32C),
            (_, Alg::Crc64Nvme) => ChecksumStrategy::full_object(Crc64Nvme),
            (Layout::FullObject, Alg::Sha1 | Alg::Sha256) => {
                panic!("S3 has no FULL_OBJECT SHA checksum: {self:?}")
            }
            (_, Alg::Sha1) => ChecksumStrategy::composite(Sha1),
            (_, Alg::Sha256) => ChecksumStrategy::composite(Sha256),
        })
    }

    /// The checksum S3 stores for the object's bytes: its algorithm, value and type. `None` for
    /// an object stored without one.
    pub(crate) fn expected_checksum(&self, seed: u32) -> Option<StoredChecksum> {
        let alg = match (self.alg, self.seeded) {
            (Alg::NoChecksum, true) => return None,
            // S3 adds CRC64NVME FULL_OBJECT to an upload that sends no checksum.
            (Alg::NoChecksum, false) => Alg::Crc64Nvme,
            (alg, _) => alg,
        };
        let digest = |offset: u64, len: u64| digest(alg, seed, offset, len);
        let (value, checksum_type) = if self.layout == Layout::Composite {
            let mut offset = 0;
            let parts: Vec<String> = self
                .stored_part_sizes()
                .into_iter()
                .map(|size| {
                    let part = digest(offset, size);
                    offset += size;
                    part
                })
                .collect();
            let value =
                s3_mock_server::composite_checksum(&parts, alg.sdk().expect("an algorithm"));
            (value, ChecksumType::Composite)
        } else {
            (digest(0, self.len), ChecksumType::FullObject)
        };
        Some(StoredChecksum {
            algorithm: alg,
            value,
            checksum_type: Some(checksum_type),
        })
    }

    /// The stored checksum of the part containing object byte `byte`: the checksum a response of
    /// that part carries. For a single PUT, the object's. `None` for an object stored without a
    /// checksum, or a byte past its end.
    pub(crate) fn part_checksum(&self, seed: u32, byte: u64) -> Option<String> {
        let alg = self.expected_checksum(seed)?.algorithm;
        let parts = self.stored_part_sizes();
        if parts.is_empty() {
            return (byte < self.len).then(|| digest(alg, seed, 0, self.len));
        }
        let mut offset = 0;
        for size in parts {
            if (offset..offset + size).contains(&byte) {
                return Some(digest(alg, seed, offset, size));
            }
            offset += size;
        }
        None
    }

    /// Uploads the object's bytes for `seed` to `key` on `target`'s backend, then checks what was
    /// stored: the requests sent (on the mock), the part sizes, and the stored checksum.
    ///
    /// On a mock target, then clears the server's request journal, so later journal checks see
    /// only the requests made after the upload.
    ///
    /// Panics, naming the object, if anything differs from the description.
    pub(crate) async fn upload(&self, target: &TmTestClient, key: &str, seed: u32) {
        let full_key = target.key(key);
        if self.seeded {
            let mock = target.mock().expect("a seeded object needs a mock target");
            let request =
                s3_mock_server::AddObjectRequest::new(object_bytes(seed, self.len as usize))
                    .checksum_algorithms([]);
            mock.add_object_with(target.bucket(), &full_key, request)
                .await
                .expect("seed object");
        } else {
            self.upload_through_transfer_manager(target, &full_key, seed)
                .await;
        }
        self.check_stored(target, &full_key, seed).await;
        if let Some(mock) = target.mock() {
            mock.clear_requests();
        }
    }

    async fn upload_through_transfer_manager(&self, target: &TmTestClient, key: &str, seed: u32) {
        let options = ClientOptions {
            part_size: self.is_multipart().then_some(self.part_size),
            // A multipart object of at least one part uploads in parts; a single PUT doesn't.
            multipart_threshold: Some(if self.is_multipart() {
                self.part_size
            } else {
                self.len + 1
            }),
            request_checksums_when_required: self.alg == Alg::NoChecksum,
            ..Default::default()
        };
        let client = target.client_with(options);
        let mut upload = client.upload().bucket(target.bucket()).key(key);
        if let Some(strategy) = self.strategy() {
            upload = upload.checksum_strategy(strategy);
        }
        let _source;
        let body = if !self.part_sizes.is_empty() {
            InputStream::from_part_stream(PrebuiltParts::new(self.prebuilt_parts(seed), self.len))
        } else if self.len > IN_MEMORY_UPLOAD_LIMIT {
            let file = tempfile::NamedTempFile::new().expect("create upload source");
            write_object_file(file.path(), seed, self.len);
            let body = InputStream::from_path(file.path()).expect("open upload source");
            _source = file;
            body
        } else {
            InputStream::from(object_bytes(seed, self.len as usize))
        };
        upload
            .body(body)
            .initiate()
            .expect("initiate upload")
            .join()
            .await
            .unwrap_or_else(|e| panic!("upload {self:?}: {}", describe_error(&e)));
    }

    fn prebuilt_parts(&self, seed: u32) -> Vec<PartData> {
        let mut offset = 0;
        self.part_sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| {
                let mut buf = vec![0; size as usize];
                fill_object_bytes(seed, offset, &mut buf);
                offset += size;
                PartData::new(i as u64 + 1, buf)
            })
            .collect()
    }

    async fn check_stored(&self, target: &TmTestClient, key: &str, seed: u32) {
        let s3 = target.s3_client().await;
        let head = s3
            .head_object()
            .bucket(target.bucket())
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap_or_else(|e| panic!("HeadObject of {self:?}: {}", describe_sdk_error(&e)));
        assert_eq!(head.content_length(), Some(self.len as i64), "{self:?}");
        assert_eq!(
            StoredChecksum::from_head(&head),
            self.expected_checksum(seed),
            "stored checksum of {self:?}"
        );

        let parts = self.stored_part_sizes();
        if !parts.is_empty() {
            for (i, size) in parts.iter().enumerate() {
                let part = target
                    .head_part(key_suffix(target, key), i as i32 + 1)
                    .await;
                assert_eq!(part.parts_count(), Some(parts.len() as i32), "{self:?}");
                assert_eq!(
                    part.content_length(),
                    Some(*size as i64),
                    "part {} of {self:?}",
                    i + 1
                );
            }
        }

        if let (Some(mock), false) = (target.mock(), self.seeded) {
            use s3_mock_server::Operation;
            let count = |op| {
                mock.requests()
                    .iter()
                    .filter(|r| r.operation == op && r.key.as_deref() == Some(key))
                    .count()
            };
            if parts.is_empty() {
                assert_eq!(count(Operation::PutObject), 1, "PutObject of {self:?}");
                assert_eq!(count(Operation::CreateMultipartUpload), 0, "{self:?}");
            } else {
                assert_eq!(count(Operation::CreateMultipartUpload), 1, "{self:?}");
                assert_eq!(
                    count(Operation::UploadPart),
                    parts.len(),
                    "UploadPart of {self:?}"
                );
                assert_eq!(count(Operation::PutObject), 0, "{self:?}");
            }
        }
    }
}

/// The base64 value of `alg` over object `seed`'s first `len` bytes.
pub(crate) fn checksum_of(alg: Alg, seed: u32, len: u64) -> String {
    Object::single_put(alg)
        .len(len)
        .expected_checksum(seed)
        .expect("a checksum algorithm")
        .value
}

/// The checksum HeadObject in checksum mode reports for logical `key`.
pub(crate) async fn stored_checksum(t: &TmTestClient, key: &str) -> Option<StoredChecksum> {
    let head = t
        .s3_client()
        .await
        .head_object()
        .bucket(t.bucket())
        .key(t.key(key))
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap_or_else(|e| panic!("HeadObject of {key}: {}", describe_sdk_error(&e)));
    StoredChecksum::from_head(&head)
}

/// Panics unless the object at logical `key` holds exactly object `seed`'s first `len` bytes.
///
/// Reads the object with one GetObject and compares it piece by piece, so the object is never held
/// in memory.
pub(crate) async fn assert_stored_bytes(t: &TmTestClient, key: &str, seed: u32, len: u64) {
    let mut body = t
        .s3_client()
        .await
        .get_object()
        .bucket(t.bucket())
        .key(t.key(key))
        .send()
        .await
        .unwrap_or_else(|e| panic!("GetObject of {key}: {}", describe_sdk_error(&e)))
        .body;
    let mut offset = 0;
    let mut expected = Vec::new();
    while let Some(chunk) = body
        .try_next()
        .await
        .unwrap_or_else(|e| panic!("read {key}: {e:?}"))
    {
        assert!(
            offset + chunk.len() as u64 <= len,
            "{key} holds more than {len} bytes"
        );
        expected.resize(chunk.len(), 0);
        fill_object_bytes(seed, offset, &mut expected);
        if let Some(i) = chunk.iter().zip(&expected).position(|(a, b)| a != b) {
            panic!(
                "{key} differs from object {seed} at byte {}",
                offset + i as u64
            );
        }
        offset += chunk.len() as u64;
    }
    assert_eq!(offset, len, "length of {key}");
}

/// Panics unless nothing is stored for logical `key`: no object, and no open multipart upload.
///
/// Reads the mock's storage on a mock target. On real S3, sends HeadObject, and in a directory
/// bucket ListMultipartUploads; in a general purpose bucket it checks only that no object is
/// stored.
pub(crate) async fn assert_nothing_stored(t: &TmTestClient, key: &str) {
    let full_key = t.key(key);
    if let Some(mock) = t.mock() {
        let exists = mock
            .object_exists(t.bucket(), &full_key)
            .await
            .expect("read mock storage");
        assert!(!exists, "an object is stored at {key}");
        let open: Vec<_> = mock
            .list_multipart_uploads(t.bucket())
            .await
            .expect("read mock storage")
            .into_iter()
            .filter(|upload| upload.key == full_key)
            .map(|upload| upload.upload_id)
            .collect();
        assert!(
            open.is_empty(),
            "multipart uploads open for {key}: {open:?}"
        );
        return;
    }

    let s3 = t.s3_client().await;
    match s3
        .head_object()
        .bucket(t.bucket())
        .key(&full_key)
        .send()
        .await
    {
        Err(e) if e.as_service_error().is_some_and(|e| e.is_not_found()) => {}
        Err(e) => panic!(
            "HeadObject of {key} must be NotFound, got {}",
            describe_sdk_error(&e)
        ),
        Ok(_) => panic!("HeadObject of {key} must be NotFound, but an object is stored"),
    }
    // FIXME(ci): add s3:ListBucketMultipartUploads on the general purpose test bucket to the CI
    // role, then check for open uploads in both bucket types.
    if t.bucket_kind() == crate::harness::BucketKind::GeneralPurpose {
        return;
    }
    // A directory bucket lists uploads only by a prefix that ends in the delimiter, so this
    // lists the test's whole prefix and filters by key.
    let uploads = s3
        .list_multipart_uploads()
        .bucket(t.bucket())
        .prefix(t.key(""))
        .send()
        .await
        .unwrap_or_else(|e| panic!("ListMultipartUploads for {key}: {}", describe_sdk_error(&e)));
    let open: Vec<_> = uploads
        .uploads()
        .iter()
        .filter(|upload| upload.key() == Some(full_key.as_str()))
        .map(|upload| upload.upload_id())
        .collect();
    assert!(
        open.is_empty(),
        "multipart uploads open for {key}: {open:?}"
    );
}

/// The base64 `alg` checksum of object `seed`'s bytes at `[offset, offset + len)`, computed in
/// pieces so the bytes are never held at once. Panics for [`Alg::NoChecksum`].
fn digest(alg: Alg, seed: u32, offset: u64, len: u64) -> String {
    let mut hasher = alg.hasher().expect("an algorithm");
    let mut buf = vec![0; (1 << 20).min(len as usize)];
    let mut done = 0;
    while done < len {
        let n = buf.len().min((len - done) as usize);
        fill_object_bytes(seed, offset + done, &mut buf[..n]);
        hasher.update(&buf[..n]);
        done += n as u64;
    }
    aws_smithy_types::base64::encode(hasher.finalize())
}

/// [`TmTestClient::head_part`] takes a logical key; `key` is already namespaced.
fn key_suffix<'a>(target: &TmTestClient, key: &'a str) -> &'a str {
    key.strip_prefix(&target.key("")).expect("a namespaced key")
}

/// Writes the bytes of object `seed`, `len` long, to `path`, in pieces.
pub(crate) fn write_object_file(path: &std::path::Path, seed: u32, len: u64) {
    use std::io::Write;
    let mut file = std::io::BufWriter::new(std::fs::File::create(path).expect("create file"));
    let mut buf = vec![0; MIB as usize];
    let mut done = 0;
    while done < len {
        let n = buf.len().min((len - done) as usize);
        fill_object_bytes(seed, done, &mut buf[..n]);
        file.write_all(&buf[..n]).expect("write file");
        done += n as u64;
    }
    file.flush().expect("flush file");
}

/// A stored checksum: its algorithm, base64 value and type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredChecksum {
    pub(crate) algorithm: Alg,
    pub(crate) value: String,
    /// The checksum type. `None` when a response reported a value without a type.
    pub(crate) checksum_type: Option<ChecksumType>,
}

impl StoredChecksum {
    /// The checksum a HeadObject in checksum mode reported: the first in [`Alg::WITH_CHECKSUM`]
    /// order, with the reported type. `None` if it reported none.
    pub(crate) fn from_head(
        head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    ) -> Option<Self> {
        let (algorithm, value) = head.first_checksum()?;
        Some(StoredChecksum {
            algorithm,
            value: value.to_owned(),
            checksum_type: head.checksum_type().cloned(),
        })
    }
}

/// A response that reports checksum values by algorithm.
pub(crate) trait ChecksumValues {
    /// The reported value for `alg`, base64. `None` if none is reported, and for
    /// [`Alg::NoChecksum`].
    fn checksum(&self, alg: Alg) -> Option<&str>;

    /// The first reported value in [`Alg::WITH_CHECKSUM`] order, with its algorithm.
    fn first_checksum(&self) -> Option<(Alg, &str)> {
        Alg::WITH_CHECKSUM
            .into_iter()
            .find_map(|alg| Some((alg, self.checksum(alg)?)))
    }
}

/// Implements [`ChecksumValues`] for a type with the SDK's five checksum accessors.
macro_rules! checksum_values {
    ($ty:ty, $crc32:ident, $crc32c:ident, $crc64nvme:ident, $sha1:ident, $sha256:ident) => {
        impl ChecksumValues for $ty {
            fn checksum(&self, alg: Alg) -> Option<&str> {
                match alg {
                    Alg::Crc32 => self.$crc32(),
                    Alg::Crc32c => self.$crc32c(),
                    Alg::Crc64Nvme => self.$crc64nvme(),
                    Alg::Sha1 => self.$sha1(),
                    Alg::Sha256 => self.$sha256(),
                    Alg::NoChecksum => None,
                }
            }
        }
    };
}
checksum_values!(
    aws_sdk_s3::operation::head_object::HeadObjectOutput,
    checksum_crc32,
    checksum_crc32_c,
    checksum_crc64_nvme,
    checksum_sha1,
    checksum_sha256
);
checksum_values!(
    aws_sdk_s3::operation::get_object::GetObjectOutput,
    checksum_crc32,
    checksum_crc32_c,
    checksum_crc64_nvme,
    checksum_sha1,
    checksum_sha256
);
checksum_values!(
    aws_sdk_s3_transfer_manager::types::IntegrityChecks,
    checksum_crc32,
    checksum_crc32c,
    checksum_crc64_nvme,
    checksum_sha1,
    checksum_sha256
);

/// A [`PartStream`] over parts built before the upload starts, with an optional full-object
/// checksum.
pub(crate) struct PrebuiltParts {
    /// The parts not yet yielded, in reverse order.
    parts: Vec<PartData>,
    size_hint: SizeHint,
    full_object_checksum: Option<String>,
}

impl PrebuiltParts {
    /// The stream of `parts`, in order, which together hold `len` bytes.
    pub(crate) fn new(mut parts: Vec<PartData>, len: u64) -> Self {
        parts.reverse();
        Self {
            parts,
            size_hint: SizeHint::exact(len),
            full_object_checksum: None,
        }
    }

    /// Reports `checksum` as the full-object checksum once the last part is yielded.
    pub(crate) fn full_object_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.full_object_checksum = Some(checksum.into());
        self
    }
}

impl PartStream for PrebuiltParts {
    fn poll_part(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        Poll::Ready(self.get_mut().parts.pop().map(Ok))
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }

    fn full_object_checksum(&self) -> Option<String> {
        self.full_object_checksum.clone()
    }
}
