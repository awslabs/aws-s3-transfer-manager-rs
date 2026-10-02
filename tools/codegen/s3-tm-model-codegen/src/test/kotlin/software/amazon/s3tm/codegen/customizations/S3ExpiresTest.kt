/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.s3tm.codegen.ModelLoader
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.BooleanShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.ShapeType
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.transform.ModelTransformer

class S3ExpiresTest {
    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `upload timestamp does not change the shared raw response or create MPU target`() {
        val original = source()
        val snapshot = original.toBuilder().build()
        val projection = TmModelProjection.project(original)
        val upload = projection.model.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        val expires = upload.getMember("Expires").orElseThrow()
        assertTrue(projection.model.expectShape(expires.target).isTimestampShape)
        assertTrue(projection.model.expectShape(TmModelProjection.id("Expires")).isStringShape)
        val create = projection.model.expectShape(TmModelProjection.id("CreateMultipartUploadRequest"), StructureShape::class.java)
        assertEquals(TmModelProjection.id("Expires"), create.getMember("Expires").orElseThrow().target)
        assertEquals(listOf(expires.id), projection.memberSources[expires.id])
        assertEquals(snapshot, original)
    }

    @Test
    fun `response strings preserve GET HEAD member correspondence under their owned name`() {
        val projection = TmModelProjection.project(source())
        for (name in listOf("ObjectMetadata", "ChunkMetadata")) {
            val metadata = projection.model.expectShape(TmModelProjection.tmId(name), StructureShape::class.java)
            assertFalse(metadata.getMember("Expires").isPresent)
            val raw = metadata.getMember("ExpiresString").orElseThrow()
            assertTrue(projection.model.expectShape(raw.target).isStringShape)
            val expected = listOf("GetObjectOutput", "HeadObjectOutput")
                .take(if (name == "ObjectMetadata") 2 else 1)
                .map { TmModelProjection.id(it).withMember("Expires") }
            assertEquals(expected, projection.memberSources[raw.id])
            assertFalse(projection.memberSources.containsKey(metadata.id.withMember("Expires")))
        }
    }

    @Test
    fun `timestamp source models still produce raw metadata strings`() {
        val original = ModelTransformer.create().changeShapeType(
            source(), mapOf(TmModelProjection.id("Expires") to ShapeType.TIMESTAMP),
        )
        val projection = TmModelProjection.project(original)
        val metadata = projection.model.expectShape(TmModelProjection.tmId("ObjectMetadata"), StructureShape::class.java)
        assertTrue(projection.model.expectShape(metadata.getMember("ExpiresString").orElseThrow().target).isStringShape)
        assertTrue(original.expectShape(TmModelProjection.id("Expires")).isTimestampShape)
    }

    @Test
    fun `unexpected expiration target types fail instead of silently changing their meaning`() {
        val invalid = source().toBuilder()
            .addShape(BooleanShape.builder().id(TmModelProjection.id("Expires")).build()).build()
        val error = assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(invalid) }
        assertTrue(error.message!!.contains("Expires has unsupported value target"))
    }

    @Test
    fun `new upstream ExpiresString members cannot silently collide`() {
        val original = source()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("ExpiresString", ShapeId.from("smithy.api#String")).build()
        val error = assertThrows(IllegalArgumentException::class.java) {
            TmModelProjection.project(original.toBuilder().addShape(head).build())
        }
        assertTrue(error.message!!.contains("ExpiresString collides"))
    }

    @Test
    fun `expiration customization is idempotent`() {
        val projection = TmModelProjection.project(source())
        assertEquals(projection, S3Expires.transform(projection))
    }
}
