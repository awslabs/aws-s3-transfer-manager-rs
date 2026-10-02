/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.smithy.diff.ModelDiff
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.traits.SensitiveTrait

class ModelProjectionTest {
    @TempDir
    lateinit var temporary: Path

    private fun source(): Model =
        ModelLoader.load(Path.of(javaClass.getResource("/s3-projection.json")!!.toURI()))

    private fun project(model: Model, directory: String = "projection"): Model =
        ModelProjection.build(model, Path.of("smithy-build.json"), temporary.resolve(directory)).model

    @Test
    fun `retains exactly the supported operations`() {
        assertEquals(
            setOf(
                "PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload",
                "AbortMultipartUpload", "GetObject", "HeadObject", "ListObjectsV2",
            ),
            project(source()).operationShapes.map { it.id.name }.toSet(),
        )
    }

    @Test
    fun `removes excluded operation dependencies and orphan shapes`() {
        val projected = project(source())
        assertTrue(
            listOf("DeleteBucket", "DeleteBucketInput", "AdminOption", "Unused").all {
                projected.getShape(ShapeId.from("com.amazonaws.s3#$it")).isEmpty
            },
        )
    }

    @Test
    fun `retains nested types lists enums and timestamps`() {
        val projected = project(source())
        assertTrue(
            listOf("GetObjectOutput", "ObjectList", "Object", "Owner", "Secret", "ChecksumAlgorithm", "LastModified").all {
                projected.getShape(ShapeId.from("com.amazonaws.s3#$it")).isPresent
            },
        )
    }

    @Test
    fun `preserves sensitive target traits`() {
        assertTrue(
            project(source()).expectShape(ShapeId.from("com.amazonaws.s3#Secret"))
                .hasTrait(SensitiveTrait::class.java),
        )
    }

    @Test
    fun `preserves shared trait schemas`() {
        val original = source()
        val trait = ShapeId.from("aws.auth#sigv4")
        assertEquals(original.expectShape(trait), project(original).expectShape(trait))
    }

    @Test
    fun `removes other services and their same-named operations`() {
        assertFalse(project(source()).getShape(ShapeId.from("example.other#GetObject")).isPresent)
    }

    @Test
    fun `excluded operation changes do not affect the projection`() {
        val original = source()
        val updated =
            original.toBuilder()
                .addShape(
                    original.expectShape(ShapeId.from("com.amazonaws.s3#AdminOption"), StringShape::class.java)
                        .toBuilder()
                        .addTrait(DocumentationTrait("Updated administration option"))
                        .build(),
                )
                .build()
        assertEquals(project(original, "old"), project(updated, "new"))
    }

    @Test
    fun `retained dependency changes affect the projection`() {
        val original = source()
        val updated =
            original.toBuilder()
                .addShape(
                    original.expectShape(ShapeId.from("com.amazonaws.s3#Secret"), StringShape::class.java)
                        .toBuilder()
                        .addTrait(DocumentationTrait("Updated object metadata"))
                        .build(),
                )
                .build()
        assertNotEquals(project(original, "old"), project(updated, "new"))
    }

    @Test
    fun `Smithy diff reports removed retained members`() {
        val original = source()
        val updated =
            original.toBuilder()
                .addShape(
                    original.expectShape(ShapeId.from("com.amazonaws.s3#Owner"), StructureShape::class.java)
                        .toBuilder()
                        .removeMember("ID")
                        .build(),
                )
                .build()
        assertTrue(
            ModelDiff.compare(project(original, "old"), project(updated, "new")).any {
                it.shapeId.orElse(null) == ShapeId.from("com.amazonaws.s3#Owner\$ID")
            },
        )
    }

    @Test
    fun `emits a model that can be loaded independently`() {
        val output = temporary.resolve("export")
        ModelProjection.build(source(), Path.of("smithy-build.json"), output)
        assertEquals(
            8,
            ModelLoader.load(output.resolve("s3-tm-dataplane/model/model.json")).operationShapes.size,
        )
    }
}
