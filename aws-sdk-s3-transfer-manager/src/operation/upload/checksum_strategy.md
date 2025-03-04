Strategy for calculating checksum values during upload.

Checksum values can be sent to S3, to verify the integrity of uploaded data.
For more information, see <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>.

You can set a specific [`ChecksumStrategy`], if you wish to choose the
checksum algorithm or already know the checksum value.

The Transfer Manager will calculate `CRC64NVME` checksums by default (if no strategy is set and the underlying
S3 client is configured with the default [`aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported`]).

To disable checksum calculation, do not set a [`ChecksumStrategy`] and make sure the underlying S3 client is
configured with the non-default [`aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired`].
S3 will still calculate and store a `CRC64NVME` full object checksum for the object server side.

If you want to provide checksum values yourself, there are several options.
You may provide the value up front via [`ChecksumStrategy::full_object_checksum`].
If you are streaming data with a [PartStream](crate::io::PartStream),
you may also provide a checksum with each [part](crate::io::PartData::with_checksum),
and may provide the [full object checksum](crate::io::PartStream::full_object_checksum)
when streaming is complete.

Checksum strings are the base64 encoding of the big endian checksum value.
