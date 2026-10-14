/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait

/** Explains response provenance and transfer-path semantics of aggregated upload metadata. */
object UploadOutputDocumentation {
    fun transform(projection: TmModelProjection.Result): TmModelProjection.Result {
        val shape = projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
        val output = shape.toBuilder().addTrait(DocumentationTrait(
            """
            Metadata and metrics for a completed upload.

            Single-request uploads use `PutObject` response metadata. Multipart uploads combine
            `CreateMultipartUpload` initiation metadata with `CompleteMultipartUpload` metadata.
            Completion replaces shared fields, including absent values; initiation-only fields
            are retained. Optional service fields are populated only when supplied by S3.
            Returned bucket/key values are response metadata, not synthesized from the request.

            Transfer Manager supplies the completion metrics. Representing returned checksum
            metadata does not imply support for calculating every modeled checksum algorithm.
            """.trimIndent(),
        ))
        shape.members().forEach { member ->
            val sources = projection.memberSources.getValue(member.id)
            val responses = sources.map { it.name.removeSuffix("Output") }.distinct()
            val upstream = member.getMemberTrait(projection.model, DocumentationTrait::class.java)
                .map { it.value }.orElse("")
            val wording = when (member.memberName) {
                "Bucket", "Key" -> {
                    val identity = if (member.memberName == "Bucket") "bucket identity" else "object key"
                    val description = if (upstream.isBlank()) "" else
                        "\n\nS3's multipart initiation documentation:\n\n$upstream"
                    "The $identity returned by S3, not synthesized from the request.$description"
                }
                else -> upstream
            }
            val presence = when {
                "PutObject" !in responses -> "Not returned for single-request uploads."
                responses == listOf("PutObject") -> "Only returned for single-request uploads."
                else -> ""
            }
            val lifecycle = when {
                "CreateMultipartUpload" in responses && "CompleteMultipartUpload" in responses ->
                    "For multipart uploads, the completion response replaces the initiation value, including when absent."
                "CreateMultipartUpload" in responses ->
                    "For multipart uploads, retained from the initiation response after successful completion."
                "PutObject" !in responses && "CompleteMultipartUpload" in responses ->
                    "Returned in the multipart completion response."
                else -> ""
            }
            val meaning = when (member.memberName) {
                "AbortDate", "AbortRuleId" ->
                    "Describes lifecycle handling of an incomplete upload at initiation, " +
                        "not a future abort of the completed object or Transfer Manager's cancellation policy."
                "ChecksumAlgorithm" ->
                    "Reports the initiation response's algorithm; it does not enable additional checksum calculation."
                "Size" ->
                    "This is append-only object size, not general transfer length. Transfer Manager does not " +
                        "initiate append uploads or populate this field from request length or transfer metrics."
                "RequestCharged" ->
                    "Reports the final request's charge status, " +
                        "not aggregate charges for initiation and part requests."
                else -> ""
            }
            val transferDocs = listOf(presence, lifecycle, meaning).filter { it.isNotBlank() }
                .joinToString("\n\n") { "<p>$it</p>" }
            val docs = listOf(wording, transferDocs).filter { it.isNotBlank() }.joinToString("\n\n")
            output.addMember(member.toBuilder().addTrait(DocumentationTrait(docs)).build())
        }
        val model = projection.model.toBuilder().addShape(output.build()).build()
        return projection.copy(model = model)
    }
}
