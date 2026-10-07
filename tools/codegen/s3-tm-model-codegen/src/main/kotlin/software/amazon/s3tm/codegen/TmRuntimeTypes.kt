/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.smithy.rust.codegen.core.smithy.RuntimeType

/** References to handwritten TM types used by the generated model surface. */
object TmRuntimeTypes {
    val inputStream = RuntimeType("crate::io::InputStream")
    val readAhead = RuntimeType("crate::types::ReadAhead")
    val checksumStrategy = RuntimeType("crate::operation::upload::ChecksumStrategy")
    val failedMultipartUploadPolicy = RuntimeType("crate::types::FailedMultipartUploadPolicy")
    val transferMetrics = RuntimeType("crate::types::TransferMetrics")
    val uploadFluentBuilder = RuntimeType("crate::operation::upload::builders::UploadFluentBuilder")
    val downloadFluentBuilder = RuntimeType("crate::operation::download::builders::DownloadFluentBuilder")
}
