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

## What is sent

With the default [`aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported`]:

- A single-request upload sends one checksum of the body: the value you provided up front via
  [`ChecksumStrategy::full_object_checksum`], or else one the transfer manager calculates as it
  uploads.
- A multipart upload sends the checksum type and a checksum with each part: the part's own
  [value](crate::io::PartData::with_checksum) if a [PartStream](crate::io::PartStream) set one, or
  else one the transfer manager calculates as it uploads that part.
- `CompleteMultipartUpload` carries a full object checksum only if you provided one, either up
  front or from [`PartStream::full_object_checksum`](crate::io::PartStream::full_object_checksum).
  Without one, S3 computes the object's checksum from the parts it stored.

The transfer manager does not itself calculate a full object checksum for a multipart upload. S3
checks each checksum it receives against the bytes it received, so only a full object value
calculated from your own source lets S3 check the assembled object against that source.
