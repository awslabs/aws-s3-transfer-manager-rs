Strategy for calculating checksum values during upload.

Checksum values can be sent to S3, to verify the integrity of uploaded data.
For more information, see <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>.

You can set a specific [`ChecksumStrategy`], if you wish to choose the
checksum algorithm or already know the checksum value.

The Transfer Manager calculates `CRC64NVME` checksums by default when no strategy
is set and the request checksum policy uses the default
[`RequestChecksumCalculation::WhenSupported`](aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenSupported).

To disable checksum calculation, do not set a [`ChecksumStrategy`] and set
[`RequestChecksumCalculation::WhenRequired`](aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired)
through [`aws_types::SdkConfig::builder`]. With `sdk-v1`, the policy can also be
set on the S3 configuration builder before conversion into
[`S3ClientConfig`](crate::config::S3ClientConfig).
S3 will still calculate and store a `CRC64NVME` full object checksum for the object server side.

If you want to provide checksum values yourself, there are several options.
You may provide the value up front via [`ChecksumStrategy::full_object_checksum`].
If you are streaming data with a [PartStream](crate::io::PartStream),
you may also provide a checksum with each [part](crate::io::PartData::with_checksum),
and may provide the [full object checksum](crate::io::PartStream::full_object_checksum)
when streaming is complete.

Checksum strings are the base64 encoding of the big endian checksum value.
