/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait

/** Adds request identifiers that are supplied by response headers rather than the S3 model. */
object RequestIdExt {
    data class Identifier(val member: String, val accessor: String, val header: String, val documentation: String)

    val identifiers = listOf(
        Identifier("RequestId", "request_id", "x-amz-request-id", "S3 request ID."),
        Identifier("ExtendedRequestId", "extended_request_id", "x-amz-id-2", "S3 extended request ID."),
    )
    val containers = listOf("ObjectMetadata", "ChunkMetadata").map(TmModelProjection::tmId)

    fun transform(projection: TmModelProjection.Result): TmModelProjection.Result {
        val builder = projection.model.toBuilder()
        val sources = projection.memberSources.toMutableMap()
        containers.forEach { id ->
            val shape = projection.model.expectShape(id, StructureShape::class.java)
            val updated = shape.toBuilder()
            identifiers.forEach { identifier ->
                require(!shape.getMember(identifier.member).isPresent) {
                    "$id ${identifier.member} collides with the synthetic request ID customization"
                }
                val member = MemberShape.builder().id(id.withMember(identifier.member))
                    .target(ShapeId.from("smithy.api#String"))
                    .addTrait(DocumentationTrait(identifier.documentation)).build()
                updated.addMember(member)
                // No upstream shape/member exists for this header-derived value.
                sources[member.id] = emptyList()
            }
            builder.addShape(updated.build())
        }
        return projection.copy(model = builder.build(), memberSources = sources.toSortedMap())
    }
}
