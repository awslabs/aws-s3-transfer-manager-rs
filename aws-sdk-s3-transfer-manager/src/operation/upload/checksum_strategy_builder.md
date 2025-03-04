Builder for [`ChecksumStrategy`].

You should prefer to use the `ChecksumStrategy::with_` constructors, instead of the builder.
The builder lets you construct a [`ChecksumStrategy`] from its constituent parts, but its `build()`
function can fail because some combinations are illegal. For example,
S3 does not allow any `SHA` algorithm to use [`ChecksumType::FullObject`] in multipart uploads,
and does not allow `CRC64NVME` to do composite multipart uploads.
The `with_` constructors cannot fail.
