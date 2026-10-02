// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
$version: "2"
namespace com.amazonaws.s3

service AmazonS3 {
    version: "2006-03-01"
    operations: [GetObject, HeadObject, ListObjectsV2, PutObject, CreateMultipartUpload, CompleteMultipartUpload]
}

operation GetObject {
    input: GetObjectRequest
    output: GetObjectOutput
}
operation HeadObject { output: HeadObjectOutput }
operation ListObjectsV2 { output: ListObjectsV2Output }
operation PutObject { input: PutObjectRequest, output: PutObjectOutput }
operation CreateMultipartUpload { input: CreateMultipartUploadRequest, output: CreateMultipartUploadOutput }
operation CompleteMultipartUpload { output: CompleteMultipartUploadOutput }

@input
structure PutObjectRequest {
    @required
    Bucket: String
    @required
    Key: String
    Body: Blob
    SSEKMSKeyId: CustomerKey
    SSEKMSEncryptionContext: CustomerKey
    ChecksumAlgorithm: ChecksumAlgorithm
    ChecksumSHA256: String
    WriteOffsetBytes: Long
    ContentLength: Long
    Expires: Expires
    AdditionalUpload: String
}
@input
structure CreateMultipartUploadRequest { Expires: Expires }
structure PutObjectOutput {
    @httpHeader("ETag")
    ETag: String
    @httpHeader("x-amz-checksum-sha256")
    ChecksumSHA256: String
    SSEKMSKeyId: CustomerKey
}
structure CreateMultipartUploadOutput {
    @xmlName("Bucket")
    Bucket: String
    UploadId: String
    AbortDate: Timestamp
}
structure CompleteMultipartUploadOutput {
    ETag: String
    ChecksumSHA256: String
    Bucket: String
    Location: String
}

@input
structure GetObjectRequest with [DownloadExtensions] {
    @required
    Bucket: String
    @required
    Key: String
    IfModifiedSince: Timestamp
    ResponseExpires: Timestamp
    SSECustomerKey: CustomerKey
    @required
    @default(1)
    PartNumber: Integer
}
@mixin
structure DownloadExtensions { InheritedOption: String }
structure GetObjectOutput with [ResponseMetadata] {
    Body: Blob
    GetOnly: String
}
structure HeadObjectOutput with [ResponseMetadata] {
    HeadOnly: String
}
@mixin
structure ResponseMetadata {
    LastModified: Timestamp
    ContentLength: Long
    ETag: String
    Metadata: Metadata
    SSEKMSKeyId: CustomerKey
    StorageClass: StorageClass
    Expires: Expires
}

string Expires
@sensitive
string CustomerKey
map Metadata { key: String, value: String }
enum StorageClass {
    STANDARD
}
structure ListObjectsV2Output { Contents: ObjectList }
list ObjectList { member: Object }
/// Object listing data.
structure Object {
    Key: String
    LastModified: Timestamp
    ETag: String
    ChecksumAlgorithm: ChecksumAlgorithmList
    ChecksumType: ChecksumType
    @default(0)
    Size: Long
    StorageClass: ObjectStorageClass
    Owner: Owner
    RestoreStatus: RestoreStatus
}
list ChecksumAlgorithmList { member: ChecksumAlgorithm }
enum ChecksumAlgorithm {
    CRC32
    @deprecated(message: "Fixture deprecated value")
    SHA256
}
enum ChecksumType {
    FULL_OBJECT
    COMPOSITE
}
enum ObjectStorageClass {
    STANDARD
    GLACIER
}
structure Owner { DisplayName: String, ID: String }
structure RestoreStatus {
    @default(false)
    IsRestoreInProgress: Boolean
    RestoreExpiryDate: Timestamp
}
