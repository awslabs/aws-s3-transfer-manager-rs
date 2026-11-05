/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.s3tm.codegen.CodegenModel
import software.amazon.s3tm.codegen.ModelLoader
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.BooleanShape
import software.amazon.smithy.model.shapes.LongShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.ClientOptionalTrait
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.InputTrait
import software.amazon.smithy.model.traits.RequiredTrait
import software.amazon.smithy.model.traits.SensitiveTrait

class S3OptionalityTest {
    private fun source() = CodegenModel.prepare(
        ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI())),
    )

    @Test
    fun `boolean and numeric member defaults are removed without changing other traits`() {
        val original = source()
        val normalized = S3Optionality.transform(original)
        for ((container, name) in listOf("Object" to "Size", "RestoreStatus" to "IsRestoreInProgress")) {
            val id = TmModelProjection.id(container).withMember(name)
            assertTrue(original.expectShape(id).hasTrait(DefaultTrait::class.java))
            assertEquals(original.expectShape(id).allTraits - DefaultTrait.ID, normalized.expectShape(id).allTraits)
        }
        assertEquals(original, source())
    }

    @Test
    fun `boolean and numeric target defaults are removed too`() {
        val targets = listOf(
            BooleanShape.builder().id(TmModelProjection.id("Flag")).addTrait(DefaultTrait(Node.from(false))).build(),
            LongShape.builder().id(TmModelProjection.id("Count")).addTrait(DefaultTrait(Node.from(0))).build(),
        )
        val container = StructureShape.builder().id(TmModelProjection.id("Values"))
        targets.forEach { container.addMember(it.id.name, it.id) { it.addTrait(SensitiveTrait()) } }
        val original = Model.builder().addShapes(targets).addShape(container.build()).build()
        val normalized = S3Optionality.transform(original)
        targets.forEach {
            assertTrue(original.expectShape(it.id).hasTrait(DefaultTrait::class.java))
            assertFalse(normalized.expectShape(it.id).hasTrait(DefaultTrait::class.java))
            assertTrue(normalized.expectShape(container.id.withMember(it.id.name)).hasTrait(SensitiveTrait::class.java))
        }
    }

    @Test
    fun `service defaulted required inputs remain omittable while required traits survive`() {
        val original = source()
        val normalized = S3Optionality.transform(original)
        val member = TmModelProjection.id("GetObjectRequest").withMember("PartNumber")
        assertTrue(normalized.expectShape(member).hasTrait(RequiredTrait::class.java))
        assertTrue(normalized.expectShape(member).hasTrait(ClientOptionalTrait::class.java))
        assertFalse(normalized.expectShape(member).hasTrait(DefaultTrait::class.java))
        val key = TmModelProjection.id("GetObjectRequest").withMember("Key")
        assertTrue(normalized.expectShape(key).hasTrait(RequiredTrait::class.java))
        assertFalse(normalized.expectShape(key).hasTrait(ClientOptionalTrait::class.java))
    }

    @Test
    fun `target defaults preserve input omission but do not remove ordinary numeric requiredness`() {
        val count = LongShape.builder().id(TmModelProjection.id("DefaultCount"))
            .addTrait(DefaultTrait(Node.from(0))).build()
        val input = StructureShape.builder().id(TmModelProjection.id("ExampleInput")).addTrait(InputTrait())
            .addMember("DefaultCount", count.id) { it.addTrait(RequiredTrait()) }
            .addMember("Count", ShapeId.from("smithy.api#Long")) { it.addTrait(RequiredTrait()) }.build()
        val long = LongShape.builder().id(ShapeId.from("smithy.api#Long")).build()
        val normalized = S3Optionality.transform(Model.builder().addShapes(count, input, long).build())
        assertTrue(normalized.expectShape(input.id.withMember("DefaultCount")).hasTrait(ClientOptionalTrait::class.java))
        val required = normalized.expectShape(input.id.withMember("Count"))
        assertTrue(required.hasTrait(RequiredTrait::class.java))
        assertFalse(required.hasTrait(ClientOptionalTrait::class.java))
    }

    @Test
    fun `string and unrelated service defaults are preserved`() {
        val string = StringShape.builder().id(TmModelProjection.id("Text"))
            .addTrait(DefaultTrait(Node.from("default"))).build()
        val other = StructureShape.builder().id(ShapeId.from("example.other#Values"))
            .addMember("Count", ShapeId.from("smithy.api#Long")) { it.addTrait(DefaultTrait(Node.from(0))) }.build()
        val values = StructureShape.builder().id(TmModelProjection.id("Values"))
            .addMember("Text", string.id) { it.addTrait(DefaultTrait(Node.from("member"))) }.build()
        val original = Model.builder().addShapes(string, values, other).build()
        assertEquals(original, S3Optionality.transform(original))
    }

    @Test
    fun `optionality normalization is idempotent`() {
        val normalized = S3Optionality.transform(source())
        assertEquals(normalized, S3Optionality.transform(normalized))
    }
}
