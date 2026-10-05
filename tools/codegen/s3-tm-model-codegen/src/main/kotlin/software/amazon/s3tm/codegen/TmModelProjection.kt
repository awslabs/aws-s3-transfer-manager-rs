/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.s3tm.codegen.customizations.RequestIdExt
import software.amazon.s3tm.codegen.customizations.S3Expires
import software.amazon.s3tm.codegen.customizations.S3Optionality
import software.amazon.s3tm.codegen.customizations.UploadOutputDocumentation
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.neighbor.Walker
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.ServiceShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.traits.InputTrait
import software.amazon.smithy.model.traits.OutputTrait
import software.amazon.smithy.model.traits.Trait
import software.amazon.smithy.rust.codegen.core.smithy.traits.SyntheticInputTrait

/** Projects service values into TM requests and aggregate response metadata. */
object TmModelProjection {
    fun id(name: String): ShapeId = ShapeId.from("com.amazonaws.s3#$name")
    fun tmId(name: String): ShapeId = ShapeId.from("s3.tm#$name")

    private fun valueTraits(member: MemberShape): Map<ShapeId, Trait> =
        member.allTraits.filterKeys { it != DocumentationTrait.ID }

    private fun aggregateTraits(shape: Shape): Map<ShapeId, Trait> =
        shape.allTraits.filterKeys {
            it != DocumentationTrait.ID && it != InputTrait.ID && it != OutputTrait.ID
        }

    val nestedRoots = listOf(
        "Object", "Owner", "RestoreStatus", "ChecksumAlgorithm", "ChecksumType", "ObjectStorageClass",
    ).map(::id)

    data class Result(
        val model: Model,
        val roots: List<ShapeId>,
        val memberSources: Map<ShapeId, List<ShapeId>>,
        val excludedMembers: Map<ShapeId, String> = emptyMap(),
    )

    fun project(dataplane: Model): Result {
        val source = S3Optionality.transform(CodegenModel.prepare(dataplane))
        nestedRoots.forEach { source.expectShape(it) }
        val get = source.expectShape(id("GetObject"), OperationShape::class.java)
        val put = source.expectShape(id("PutObject"), OperationShape::class.java)
        val output = source.expectShape(get.output.orElseThrow(), StructureShape::class.java)
        val provenance = sortedMapOf<ShapeId, List<ShapeId>>()
        val exclusions = sortedMapOf<ShapeId, String>()
        val download = input(source, get, "downloading", provenance, exclusions)
        val upload = input(source, put, "uploading", provenance, exclusions, UploadRequestPolicy::exposureExclusion)
        fun operationOutput(name: String) = source.expectShape(
            source.expectShape(id(name), OperationShape::class.java).output.orElseThrow(), StructureShape::class.java,
        )
        val metadata = aggregate(
            "ObjectMetadata", listOf(output, operationOutput("HeadObject")), true, provenance, exclusions,
        )
        val chunk = aggregate("ChunkMetadata", listOf(output), true, provenance, exclusions)
        val uploadOutput = aggregate(
            "UploadOutput", listOf("PutObject", "CreateMultipartUpload", "CompleteMultipartUpload").map(::operationOutput),
            false, provenance, exclusions,
        )
        val service = source.expectShape(ModelLoader.serviceId, ServiceShape::class.java)
            .toBuilder().putRename(download.id, "DownloadInput").putRename(upload.id, "UploadInput").build()
        val projected = source.toBuilder()
            .addShapes(download, upload, metadata, chunk, uploadOutput, service).build()
        val roots = nestedRoots + listOf(download.id, upload.id, metadata.id, chunk.id, uploadOutput.id)
        val customized = RequestIdExt.transform(S3Expires.transform(Result(projected, roots, provenance, exclusions)))
        val memberSources = customized.memberSources.toSortedMap()
        collectCorrespondence(customized.model, roots, memberSources)
        return UploadOutputDocumentation.transform(customized.copy(memberSources = memberSources))
    }

    private fun input(
        source: Model,
        operation: OperationShape,
        action: String,
        provenance: MutableMap<ShapeId, List<ShapeId>>,
        exclusions: MutableMap<ShapeId, String>,
        exclusion: (MemberShape) -> String? = { null },
    ): StructureShape {
        val original = source.expectShape(operation.input.orElseThrow(), StructureShape::class.java)
        val result = original.toBuilder()
            .addTrait(SyntheticInputTrait(operation.id, original.id))
            .addTrait(DocumentationTrait("Input for $action a single S3 object."))
        original.members().forEach { member ->
            val reason = exclusion(member)
            if (reason == null) {
                provenance[member.id] = listOf(member.id)
            } else {
                result.removeMember(member.memberName)
                exclusions[member.id] = reason
            }
        }
        return result.build()
    }

    private fun aggregate(
        name: String,
        outputs: List<StructureShape>,
        omitBody: Boolean,
        provenance: MutableMap<ShapeId, List<ShapeId>>,
        exclusions: MutableMap<ShapeId, String>,
    ): StructureShape {
        val result = StructureShape.builder().id(tmId(name))
            .addTrait(DocumentationTrait("S3 response metadata for $name."))
        val sharedTraits = aggregateTraits(outputs.first())
        require(outputs.all { aggregateTraits(it) == sharedTraits }) {
            "$name response structure value traits differ; an explicit value projection policy is required"
        }
        sharedTraits.values.forEach { result.addTrait(it) }
        outputs.flatMap { it.members() }.groupBy { it.memberName }.toSortedMap().forEach { (memberName, originals) ->
            if (omitBody && memberName == "Body") {
                originals.forEach { exclusions[it.id] = "Response body belongs to the transfer stream." }
            } else {
                val original = originals.first()
                require(originals.all { it.target == original.target && valueTraits(it) == valueTraits(original) }) {
                    "$name metadata differs for $memberName (${originals.joinToString { it.id.toString() }}); " +
                        "an explicit value projection policy is required"
                }
                val member = original.toBuilder().id(tmId(name).withMember(memberName)).build()
                result.addMember(member)
                provenance[member.id] = originals.map { it.id }
            }
        }
        return result.build()
    }

    private fun collectCorrespondence(
        model: Model,
        roots: List<ShapeId>,
        provenance: MutableMap<ShapeId, List<ShapeId>>,
    ) {
        // Record ordinary nested values too, rather than only the synthesized roots.
        roots.flatMap { Walker(model).walkShapes(model.expectShape(it)) }
            .filterIsInstance<MemberShape>().forEach { provenance.putIfAbsent(it.id, listOf(it.id)) }
    }
}
