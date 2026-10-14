/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
@file:Suppress("DEPRECATION")

package software.amazon.s3tm.codegen.sdkv1

import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.s3tm.codegen.UploadRequestPolicy
import software.amazon.smithy.model.neighbor.Walker
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.EnumShape
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.EnumTrait

/** Model-derived correspondence and explicit ownership of non-ordinary request fields. */
class SdkV1Mapping(val projection: TmModelProjection.Result, val symbols: SdkV1Symbols) {
    data class Member(val tm: MemberShape, val sdk: MemberShape)
    data class Request(val operation: OperationShape, val input: StructureShape, val members: List<Member>)
    data class Response(val operation: OperationShape, val output: StructureShape, val members: List<Member>)

    val shapes = projection.roots.flatMap { Walker(projection.model).walkShapes(projection.model.expectShape(it)) }
        .distinctBy { it.id }.sortedBy { it.id }
    val enums = shapes.filter(::isEnum)
    private val operationRoots = projection.roots.drop(TmModelProjection.nestedRoots.size).toSet()
    val values = shapes.filterIsInstance<StructureShape>().filter { it.id !in operationRoots }
    private val coverage = sortedMapOf<String, String>()
    private val reviewIssues = mutableListOf<String>()

    fun valueMembers(shape: StructureShape): List<Member> = shape.members().map { member ->
        val source = projection.memberSources[member.id]?.singleOrNull()
            ?: error("Nested value ${member.id} needs one SDK source member")
        Member(member, symbols.sdkModel.expectShape(source, MemberShape::class.java))
    }

    private fun request(operation: String, tmInput: String): Request? {
        val op = symbols.sdkModel.getShape(TmModelProjection.id(operation)).orElse(null) as? OperationShape ?: return null
        if (op.input.isEmpty) return null // Reduced model fixtures can omit an operation input.
        val input = projection.model.expectShape(TmModelProjection.id(tmInput), StructureShape::class.java)
        val sdkInput = symbols.sdkModel.expectShape(op.input.orElseThrow(), StructureShape::class.java)
        val upload = tmInput == "PutObjectRequest"
        if (upload) reviewIssues += UploadRequestPolicy.staleEntries(operation, sdkInput)
        val put = symbols.sdkModel.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        val members = sdkInput.members().mapNotNull { member ->
            val route = if (upload) UploadRequestPolicy.route(operation, member, put) else {
                if (member.memberName == "PartNumber") UploadRequestPolicy.Route.Controlled("Download part/range execution.")
                else UploadRequestPolicy.Route.Forward(input.id.withMember(member.memberName))
            }
            when (route) {
                is UploadRequestPolicy.Route.Controlled -> {
                    coverage[member.id.toString()] = "controlled: ${route.reason}"
                    null
                }
                is UploadRequestPolicy.Route.Omit -> {
                    coverage[member.id.toString()] = "omitted: ${route.reason}"
                    null
                }
                is UploadRequestPolicy.Route.ReviewRequired -> {
                    reviewIssues += "${member.id}: ${route.reason}"
                    null
                }
                is UploadRequestPolicy.Route.Forward -> {
                    val tmMember = input.getMember(route.sourceMember.member.orElseThrow()).orElse(null)
                        ?: error("${member.id} has no TM input member or explicit ownership policy")
                    require(tmMember.target == member.target || member.memberName == "Expires") {
                        "Request target differs: ${member.id} -> ${tmMember.id}"
                    }
                    coverage[member.id.toString()] = "forwarded: ${tmMember.id}"
                    Member(tmMember, member)
                }
            }
        }
        return Request(op, input, members)
    }

    val requests = listOf(
        "PutObject" to "PutObjectRequest", "CreateMultipartUpload" to "PutObjectRequest",
        "UploadPart" to "PutObjectRequest", "CompleteMultipartUpload" to "PutObjectRequest",
        "AbortMultipartUpload" to "PutObjectRequest", "GetObject" to "GetObjectRequest",
        "HeadObject" to "GetObjectRequest",
    ).mapNotNull { (operation, input) -> request(operation, input) }

    private fun response(operation: String, tmOutput: String): Response? {
        val op = symbols.sdkModel.getShape(TmModelProjection.id(operation)).orElse(null) as? OperationShape ?: return null
        if (op.output.isEmpty) return null
        val sdkOutput = symbols.sdkModel.expectShape(op.output.orElseThrow(), StructureShape::class.java)
        val output = projection.model.expectShape(TmModelProjection.tmId(tmOutput), StructureShape::class.java)
        val members = sdkOutput.members().mapNotNull { member ->
            if (member.memberName == "Body") {
                coverage[member.id.toString()] = "TM-controlled: response stream"
                null
            } else {
                val tmMember = output.members().singleOrNull {
                    projection.memberSources[it.id].orEmpty().contains(member.id)
                } ?: error("${member.id} has no $tmOutput member correspondence")
                coverage[member.id.toString()] = "forwarded: ${tmMember.id}"
                Member(tmMember, member)
            }
        }
        return Response(op, output, members)
    }

    val responses = listOf(
        "GetObject" to "ObjectMetadata", "HeadObject" to "ObjectMetadata",
        "GetObject" to "ChunkMetadata", "PutObject" to "UploadOutput",
        "CreateMultipartUpload" to "UploadOutput", "CompleteMultipartUpload" to "UploadOutput",
    ).mapNotNull { (operation, output) -> response(operation, output) }

    init {
        require(reviewIssues.isEmpty()) { "Upload mapping requires review:\n${reviewIssues.sorted().joinToString("\n")}" }
        values.forEach { shape -> valueMembers(shape).forEach { coverage[it.sdk.id.toString()] = "value: ${it.tm.id}" } }
        symbols.policy.fields.forEach { (shape, fields) ->
            fields.filter { it.runtime != null }.forEach { coverage["$shape\$${it.name}"] = "TM-only: ${it.runtime!!.type.path}" }
        }
        projection.excludedMembers.forEach { (member, reason) -> coverage.putIfAbsent(member.toString(), "excluded: $reason") }
    }

    fun report(): String = Node.prettyPrintJson(Node.objectNodeBuilder()
        .withMember("schemaVersion", 1)
        .withMember("members", Node.objectNodeBuilder().also { builder ->
            coverage.forEach { (member, classification) -> builder.withMember(member, classification) }
        }.build()).build()) + "\n"

    companion object {
        fun isEnum(shape: Shape): Boolean = shape is EnumShape || shape.isStringShape && shape.hasTrait(EnumTrait::class.java)
    }
}
