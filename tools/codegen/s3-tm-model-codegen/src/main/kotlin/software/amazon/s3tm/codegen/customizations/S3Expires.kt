/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.ShapeType
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.shapes.TimestampShape

/** Keeps upload expiration timestamps and raw response expiration strings independent. */
object S3Expires {
    fun transform(projection: TmModelProjection.Result): TmModelProjection.Result {
        val model = projection.model
        val builder = model.toBuilder()
        val sources = projection.memberSources.toMutableMap()
        val upload = model.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        upload.getMember("Expires").orElse(null)?.let { member ->
            val target = model.expectShape(member.target)
            require(target.type in setOf(ShapeType.STRING, ShapeType.TIMESTAMP)) {
                "UploadInput Expires has unsupported value target ${target.id}: ${target.type}"
            }
            val timestamp = TimestampShape.builder().id(TmModelProjection.tmId("UploadExpires"))
            target.allTraits.values.forEach { timestamp.addTrait(it) }
            val timestampShape = timestamp.build()
            builder.addShape(timestampShape)
            builder.addShape(upload.toBuilder().addMember(member.toBuilder().target(timestampShape.id).build()).build())
        }
        for (name in listOf("ObjectMetadata", "ChunkMetadata")) {
            val shape = model.expectShape(TmModelProjection.tmId(name), StructureShape::class.java)
            val member = shape.getMember("Expires").orElse(null) ?: continue
            require(!shape.getMember("ExpiresString").isPresent) {
                "$name ExpiresString collides with the raw Expires customization"
            }
            val target = model.expectShape(member.target)
            require(target.type in setOf(ShapeType.STRING, ShapeType.TIMESTAMP)) {
                "$name Expires has unsupported value target ${target.id}: ${target.type}"
            }
            val raw = StringShape.builder().id(TmModelProjection.tmId("RawExpires"))
            target.allTraits.values.forEach { raw.addTrait(it) }
            val rawShape = raw.build()
            builder.addShape(rawShape)
            val renamed = member.toBuilder().id(shape.id.withMember("ExpiresString")).target(rawShape.id).build()
            builder.addShape(shape.toBuilder().removeMember("Expires").addMember(renamed).build())
            sources[renamed.id] = sources.remove(member.id) ?: listOf(member.id)
        }
        return projection.copy(model = builder.build(), memberSources = sources.toSortedMap())
    }
}
