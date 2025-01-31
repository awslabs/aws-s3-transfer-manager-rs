Strategy for calculating checksum values during upload.

Checksum values can be sent to S3, to verify the integrity of uploaded data.
For more information, see <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>.

You can set a specific [`ChecksumStrategy`], if you wish to choose the
checksum algorithm or already know the checksum value.

`CRC64NVME` checksums are calculated by default (if no strategy is set and the underlying
S3 client is configured with the default [`aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported`]).

To disable checksum calculation, do not set a [`ChecksumStrategy`] and make sure the underlying S3 client is
configured with the non-default [`aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired`].
If you do this, S3 will still calculate and store a `CRC64NVME` full object checksum for the object.
