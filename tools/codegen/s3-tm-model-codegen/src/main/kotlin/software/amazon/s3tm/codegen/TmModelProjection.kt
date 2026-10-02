/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.ServiceShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.transform.ModelTransformer
import software.amazon.smithy.rust.codegen.core.smithy.traits.SyntheticInputTrait

/** TM value projections retain the source members' targets and traits. */
object TmModelProjection {
    fun id(name: String): ShapeId = ShapeId.from("com.amazonaws.s3#$name")

    val nestedRoots = listOf(
        "Object", "Owner", "RestoreStatus", "ChecksumAlgorithm", "ChecksumType", "ObjectStorageClass",
    ).map(::id)

    data class Result(
        val model: Model,
        val roots: List<ShapeId>,
        val memberSources: Map<ShapeId, List<ShapeId>>,
    )

    fun project(dataplane: Model): Result {
        val source = ModelTransformer.create().flattenAndRemoveMixins(dataplane)
        nestedRoots.forEach { source.expectShape(it) }
        val get = source.expectShape(id("GetObject"), OperationShape::class.java)
        val head = source.expectShape(id("HeadObject"), OperationShape::class.java)
        val input = source.expectShape(get.input.orElseThrow(), StructureShape::class.java)
        val output = source.expectShape(get.output.orElseThrow(), StructureShape::class.java)
        val headOutput = source.expectShape(head.output.orElseThrow(), StructureShape::class.java)
        val provenance = sortedMapOf<ShapeId, List<ShapeId>>()
        val download = input.toBuilder()
            .addTrait(SyntheticInputTrait(get.id, input.id))
            .addTrait(DocumentationTrait("Input for downloading a single S3 object."))
        input.members().forEach { member ->
            provenance[member.id] = listOf(member.id)
        }
        val metadataId = ShapeId.from("s3.tm#ObjectMetadata")
        val metadata = StructureShape.builder().id(metadataId)
            .addTrait(DocumentationTrait("S3 object response metadata, excluding the response body."))
        // Response bodies belong to the transfer stream rather than object metadata.
        val names = (output.allMembers.keys + headOutput.allMembers.keys) - "Body"
        names.sorted().forEach { name ->
            val originals = listOfNotNull(
                output.getMember(name).orElse(null),
                headOutput.getMember(name).orElse(null),
            )
            val original = originals.first()
            require(originals.all {
                it.target == original.target &&
                    it.allTraits - DocumentationTrait.ID == original.allTraits - DocumentationTrait.ID
            }) {
                "GET/HEAD metadata differs for $name; an explicit projection policy is required"
            }
            val member = original.toBuilder().id(metadataId.withMember(name)).build()
            metadata.addMember(member)
            provenance[member.id] = originals.map { it.id }
        }
        val service = source.expectShape(ModelLoader.serviceId, ServiceShape::class.java)
            .toBuilder().putRename(input.id, "DownloadInput").build()
        val projected = source.toBuilder().addShapes(download.build(), metadata.build(), service).build()
        return Result(projected, nestedRoots + listOf(input.id, metadataId), provenance)
    }
}
