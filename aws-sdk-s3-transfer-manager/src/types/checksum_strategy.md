How an upload's checksums are chosen.

S3 checks each checksum an upload sends against the bytes it receives, and stores a checksum with
the object. See [Checking object integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html).
A strategy answers three questions:

1. **Which algorithm?** S3 stores the object's checksum in this algorithm, named by a type from
   [`checksum`](crate::types::checksum).
2. **Should the stored checksum cover the whole object?**
   - [`full_object`](ChecksumStrategy::full_object): the stored checksum equals a checksum of the
     whole object, so it can be compared with one computed elsewhere. CRC64NVME, CRC32 or CRC32C:
     the [`FullObjectAlgorithm`](crate::types::checksum::FullObjectAlgorithm) types.
   - [`composite`](ChecksumStrategy::composite): for a multipart upload, the stored checksum is a
     checksum of the part checksums. It depends on the part size and doesn't equal a checksum of
     the object. CRC32, CRC32C, SHA1 or SHA256: the
     [`CompositeAlgorithm`](crate::types::checksum::CompositeAlgorithm) types. A single-request
     upload sends a full object checksum of the same algorithm.

   A constructor called with an algorithm its checksum type doesn't support doesn't compile.
3. **Do you know the expected checksum?**
   [`with_expected_value`](ChecksumStrategy::with_expected_value) gives S3 the value to check the
   uploaded object against; S3 rejects the upload if it doesn't match. A single-request upload
   sends the value in place of a checksum calculated from the body. A multipart upload still
   calculates each part's checksum and also sends the value with `CompleteMultipartUpload`.
   Supply one to check the stored object against its source: the checksums the transfer manager
   calculates cover only the bytes it sends, so they don't detect a source that changed or was
   read incorrectly. For a full object strategy, a [`PartStream`](crate::io::PartStream) can
   instead return the value from
   [`PartStream::full_object_checksum`](crate::io::PartStream::full_object_checksum) after its
   final part.

The transfer manager calculates a checksum of each body it sends: the body of a single-request
upload, and each part of a multipart upload. It doesn't calculate a value you supply, with
`with_expected_value` or through a `PartStream`. It never calculates the object-level checksum of
a multipart upload: that value comes from you, or S3 derives it from the part checksums.

# Default

An upload without a strategy uses [`ChecksumStrategy::default()`], a full object CRC64NVME
strategy, when the request checksum policy is the default
[`RequestChecksumCalculation::WhenSupported`](aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenSupported).
Under
[`RequestChecksumCalculation::WhenRequired`](aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired),
an upload without a strategy sends no checksums; S3 still stores a full object CRC64NVME checksum
for the object. A strategy set on the upload is used under either policy.

Set the policy through [`aws_types::SdkConfig::builder`]. With `sdk-v1`, it can also be set on the
S3 configuration builder before conversion into [`S3ClientConfig`](crate::config::S3ClientConfig).

# What is sent

A single-request upload sends one checksum of the body: the expected value, or one calculated as
the body is sent.

A multipart upload:

1. `CreateMultipartUpload` declares the algorithm and the type.
2. Each `UploadPart` carries its part's checksum: the value a `PartStream` set with
   [`PartData::with_checksum`](crate::io::PartData::with_checksum), or else one calculated as the
   part is sent.
3. `CompleteMultipartUpload` carries the type, each part's checksum and, for a full object
   strategy, the expected value if there is one.
4. S3 derives the object's checksum from the part checksums. With an expected value, a mismatch
   fails the upload.

# Rules

- A composite strategy with an expected value is valid only for a single-request upload.
  `initiate` fails with [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) before
  any request is sent when the body's size has no upper bound, the body is a `PartStream`, or its
  upper bound is at least the multipart threshold.
- Number a `PartStream`'s parts consecutively from 1. S3 requires it for parts that carry some
  checksums and fails `CompleteMultipartUpload` otherwise; see
  [`PartData::new`](crate::io::PartData::new).
- Checksum values are the base64 encoding of the big-endian checksum.

# Downloads

A full object CRC lets a download, including one split into ranges, check the whole object it
received. A composite checksum allows checks of responses that align with stored parts, not of
the object as a whole. See [`ChecksumValidation`](crate::types::ChecksumValidation).
