/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.ClientOptionalTrait
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.traits.RequiredTrait

/** PUT supplies public fields; other upload paths use reviewed forwarding rules. */
object UploadRequestPolicy {
    sealed interface Route {
        data class Forward(val sourceMember: ShapeId) : Route
        data class Controlled(val reason: String) : Route
        data class Omit(val reason: String) : Route
        data class ReviewRequired(val reason: String) : Route
    }

    data class Rules(val forward: Set<String>, val overrides: Map<String, Route>) {
        init {
            require((forward intersect overrides.keys).isEmpty()) { "Conflicting upload forwarding rules" }
        }
    }

    private val context = setOf("Bucket", "Key", "ExpectedBucketOwner", "RequestPayer")
    private val customerEncryption = setOf("SSECustomerAlgorithm", "SSECustomerKey", "SSECustomerKeyMD5")
    private val objectSetup = setOf(
        "ACL", "BucketKeyEnabled", "CacheControl", "ContentDisposition", "ContentEncoding",
        "ContentLanguage", "ContentType", "Expires", "GrantFullControl", "GrantRead", "GrantReadACP",
        "GrantWriteACP", "Metadata", "ObjectLockLegalHoldStatus", "ObjectLockMode",
        "ObjectLockRetainUntilDate", "SSEKMSEncryptionContext", "SSEKMSKeyId",
        "ServerSideEncryption", "StorageClass", "Tagging", "WebsiteRedirectLocation",
    )
    private val checksumValues = setOf(
        "ChecksumCRC32", "ChecksumCRC32C", "ChecksumCRC64NVME", "ChecksumSHA1", "ChecksumSHA256",
        "ChecksumMD5", "ChecksumSHA512", "ChecksumXXHASH64", "ChecksumXXHASH3", "ChecksumXXHASH128",
    )
    private val checksums = checksumValues + setOf("ChecksumAlgorithm", "ChecksumType")
    private fun controlled(names: Set<String>, reason: String) =
        names.associateWith { Route.Controlled(reason) }
    private fun checksum(names: Set<String>) = controlled(
        names, "ChecksumStrategy and per-part checksum handling, not ordinary forwarding.",
    )

    val rules = mapOf(
        "PutObject" to Rules(emptySet(),
            controlled(setOf("Body", "ContentLength"), "Transfer stream and request-specific length.") +
                checksum(checksumValues + "ChecksumAlgorithm") +
                mapOf("WriteOffsetBytes" to Route.Omit("Append uploads are not supported."))),
        "CreateMultipartUpload" to Rules(context + customerEncryption + objectSetup,
            checksum(setOf("ChecksumAlgorithm", "ChecksumType"))),
        "UploadPart" to Rules(context + customerEncryption,
            controlled(setOf("Body", "ContentLength", "PartNumber", "UploadId"),
                "Transfer stream, request length and multipart identity.") +
                checksum(checksumValues + "ChecksumAlgorithm") +
                mapOf("ContentMD5" to Route.Omit("A whole-object MD5 cannot be reused for each part."))),
        "CompleteMultipartUpload" to Rules(context + customerEncryption + setOf("IfMatch", "IfNoneMatch"),
            controlled(setOf("MultipartUpload", "MpuObjectSize", "UploadId"),
                "Completed parts, source-size accounting and multipart identity.") +
                checksum(checksumValues + "ChecksumType")),
        "AbortMultipartUpload" to Rules(context,
            controlled(setOf("UploadId"), "Multipart identity tracked by the transfer.") +
                mapOf("IfMatchInitiatedTime" to Route.Omit(
                    "Abort-specific conditional parameter is not exposed; the SDK remains available.",
                ))),
    )

    fun exposureExclusion(member: MemberShape): String? = when {
        member.memberName in checksums -> "Checksum selection and values are controlled by TM ChecksumStrategy."
        member.memberName.startsWith("Checksum") ->
            error("${member.id}: new checksum member requires an upload exposure policy")
        member.memberName == "WriteOffsetBytes" -> "Append uploads are not supported by TM."
        else -> null
    }

    /** Requiredness and defaulting are checked per operation, not merged into public requirements. */
    private fun valueTraits(member: MemberShape) = member.allTraits.filterKeys {
        it !in setOf(DocumentationTrait.ID, RequiredTrait.ID, DefaultTrait.ID, ClientOptionalTrait.ID)
    }

    fun route(operation: String, member: MemberShape, put: StructureShape): Route {
        val policy = rules.getValue(operation)
        policy.overrides[member.memberName]?.let { return it }
        val source = put.getMember(member.memberName).orElse(null)
        if (operation != "PutObject" && member.memberName !in policy.forward) {
            return Route.ReviewRequired(
                "Routing is not reviewed; target=${member.target}; PUT counterpart=${source?.id ?: "none"}.",
            )
        }
        if (source == null) return Route.ReviewRequired("No PUT-derived source for ${member.target}.")
        if (source.target != member.target || valueTraits(source) != valueTraits(member)) {
            return Route.ReviewRequired("Incompatible PUT source ${source.id}: ${source.target} -> ${member.target}.")
        }
        return Route.Forward(source.id)
    }

    fun staleEntries(operation: String, input: StructureShape): List<String> {
        val policy = rules.getValue(operation)
        return ((policy.forward + policy.overrides.keys) - input.allMembers.keys).sorted().map {
            "${input.id.withMember(it)}: reviewed policy member no longer exists."
        }
    }
}
