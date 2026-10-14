/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.ModelGenerator
import software.amazon.s3tm.codegen.ModelLoader
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait

class UploadOutputDocumentationTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `aggregate preserves service meaning and documents transfer path and lifecycle`() {
        val original = source()
        val put = original.expectShape(TmModelProjection.id("PutObjectOutput"), StructureShape::class.java)
            .toBuilder()
            .addMember("ChecksumSHA512", ShapeId.from("smithy.api#String")) {
                it.addTrait(DocumentationTrait("Original checksum limitation."))
            }
            .addMember("Size", ShapeId.from("smithy.api#Long"))
            .build()
        val complete = original.expectShape(TmModelProjection.id("CompleteMultipartUploadOutput"), StructureShape::class.java)
            .toBuilder().addMember("ChecksumSHA512", ShapeId.from("smithy.api#String")).build()
        val create = original.expectShape(TmModelProjection.id("CreateMultipartUploadOutput"), StructureShape::class.java)
        val bucket = create.getMember("Bucket").orElseThrow().toBuilder()
            .addTrait(DocumentationTrait("Initiation-specific caveat.")).build()
        val updatedCreate = create.toBuilder().addMember(bucket)
            .addMember("ChecksumAlgorithm", TmModelProjection.id("ChecksumAlgorithm")).build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(put, complete, updatedCreate).build())
        val shape = projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
        fun docs(name: String) = shape.getMember(name).orElseThrow().expectTrait(DocumentationTrait::class.java).value
        assertTrue(shape.expectTrait(DocumentationTrait::class.java).value.contains("including absent values"))
        assertTrue(docs("ChecksumSHA512").contains("Original checksum limitation."))
        assertEquals(
            listOf(put.id.withMember("ChecksumSHA512"), complete.id.withMember("ChecksumSHA512")),
            projection.memberSources.getValue(shape.id.withMember("ChecksumSHA512")),
        )
        assertTrue(docs("AbortDate").contains("not a future abort of the completed object"))
        assertTrue(docs("Bucket").contains("not synthesized from the request"))
        assertTrue(docs("Bucket").contains("including when absent"))
        assertTrue(docs("Bucket").contains("Initiation-specific caveat."))
        assertTrue(docs("Size").contains("append-only object size, not general transfer length"))
        assertTrue(docs("ChecksumAlgorithm").contains("initiation response's algorithm"))
        val generated = ModelGenerator.generate(projection, temporary, "1.6.3")
        val rust = Files.readString(generated.baseDir.resolve("src/model/_upload_output.rs"))
        assertFalse(rust.contains("Source responses:"))
        assertTrue(rust.contains("Original checksum limitation."))
    }

    @Test
    fun `additive members acquire transfer path docs without a field inventory`() {
        val original = source()
        val create = original.expectShape(TmModelProjection.id("CreateMultipartUploadOutput"), StructureShape::class.java)
            .toBuilder().addMember("FutureMetadata", ShapeId.from("smithy.api#String")).build()
        val projection = TmModelProjection.project(original.toBuilder().addShape(create).build())
        val member = projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
            .getMember("FutureMetadata").orElseThrow()
        assertEquals(listOf(create.id.withMember("FutureMetadata")), projection.memberSources.getValue(member.id))
        val docs = member.expectTrait(DocumentationTrait::class.java).value
        assertTrue(docs.contains("For multipart uploads"))
        assertTrue(docs.contains("Not returned for single-request uploads"))
        assertTrue(docs.contains("retained from the initiation response"))
    }

    @Test
    fun `target documentation is inherited and member documentation takes precedence`() {
        val original = source()
        val target = StringShape.builder().id(TmModelProjection.id("RequestCharged"))
            .addTrait(DocumentationTrait(
                "If present, indicates that the requester was charged. Not supported for directory buckets.",
            )).build()
        val create = original.expectShape(TmModelProjection.id("CreateMultipartUploadOutput"), StructureShape::class.java)
            .toBuilder().addMember("RequestCharged", target.id).build()
        val complete = original.expectShape(TmModelProjection.id("CompleteMultipartUploadOutput"), StructureShape::class.java)
            .toBuilder().addMember("RequestCharged", target.id).build()
        for (memberDocs in listOf(null, "Member-specific charge meaning.")) {
            val put = original.expectShape(TmModelProjection.id("PutObjectOutput"), StructureShape::class.java)
                .toBuilder().addMember("RequestCharged", target.id) {
                    if (memberDocs != null) it.addTrait(DocumentationTrait(memberDocs))
                }.build()
            val projection = TmModelProjection.project(original.toBuilder().addShapes(target, put, create, complete).build())
            val member = projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
                .getMember("RequestCharged").orElseThrow()
            val docs = member.expectTrait(DocumentationTrait::class.java).value
            val expected = memberDocs ?: target.expectTrait(DocumentationTrait::class.java).value
            assertTrue(docs.startsWith(expected))
            assertTrue(docs.contains("completion response replaces the initiation value, including when absent"))
            assertTrue(docs.contains("not aggregate charges"))
            assertFalse(docs.contains("Source responses:"))
            if (memberDocs != null) assertFalse(docs.contains("Not supported for directory buckets"))
            val generated = ModelGenerator.generate(projection, temporary.resolve(if (memberDocs == null) "inherited" else "member"), "1.6.3")
            val rust = Files.readString(generated.baseDir.resolve("src/model/_upload_output.rs"))
            assertTrue(rust.contains(expected))
        }
    }
}
