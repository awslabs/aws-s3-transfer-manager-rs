/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.traits.DynamicTrait
import software.amazon.smithy.model.traits.HttpHeaderTrait
import software.amazon.smithy.model.traits.RequiredTrait
import software.amazon.smithy.model.traits.SensitiveTrait

class CodegenModelTest {
    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `flattens mixins while retaining member identity and value traits`() {
        val original = source()
        val prepared = CodegenModel.prepare(original)
        val inputId = TmModelProjection.id("GetObjectRequest")
        val before = original.expectShape(inputId, StructureShape::class.java)
        val after = prepared.expectShape(inputId, StructureShape::class.java)
        assertFalse(before.mixins.isEmpty())
        assertTrue(after.mixins.isEmpty())
        assertEquals(before.allMembers.keys, after.allMembers.keys)
        assertFalse(before.getMember("InheritedOption").orElseThrow().mixins.isEmpty())
        before.members().forEach { member ->
            val flattened = after.getMember(member.memberName).orElseThrow()
            assertEquals(member.id, flattened.id)
            assertEquals(member.target, flattened.target)
            assertEquals(member.allTraits, flattened.allTraits)
            assertTrue(flattened.mixins.isEmpty())
        }
        assertFalse(prepared.getShape(TmModelProjection.id("DownloadExtensions")).isPresent)
        val part = after.getMember("PartNumber").orElseThrow()
        assertTrue(part.hasTrait(RequiredTrait::class.java))
        assertEquals(
            before.getMember("PartNumber").orElseThrow().expectTrait(DefaultTrait::class.java),
            part.expectTrait(DefaultTrait::class.java),
        )
        val customerKey = TmModelProjection.id("CustomerKey")
        assertEquals(original.expectShape(customerKey), prepared.expectShape(customerKey))
        assertTrue(prepared.expectShape(customerKey).hasTrait(SensitiveTrait::class.java))
        val checksum = TmModelProjection.id("ChecksumAlgorithm")
        assertEquals(original.expectShape(checksum), prepared.expectShape(checksum))
    }

    @Test
    fun `removes every wire trait across containers members and targets without removing value traits`() {
        // Exercise the normalization policy independently of each trait's binding-site validation.
        val traits = listOf(
            "httpHeader", "httpPrefixHeaders", "httpPayload", "httpLabel", "httpQuery",
            "httpQueryParams", "httpResponseCode", "xmlName", "xmlAttribute", "xmlFlattened",
            "xmlNamespace", "timestampFormat", "jsonName",
        ).map { DynamicTrait(ShapeId.from("smithy.api#$it"), Node.objectNode()) }
        val targetBuilder = StringShape.builder().id(TmModelProjection.id("WireValue"))
            .addTrait(SensitiveTrait()).addTrait(DocumentationTrait("Value documentation"))
        traits.forEach { targetBuilder.addTrait(it) }
        val target = targetBuilder.build()
        val structureBuilder = StructureShape.builder().id(TmModelProjection.id("WireContainer"))
            .addTrait(DocumentationTrait("Container documentation"))
            .addMember("Value", target.id) { member ->
                member.addTrait(RequiredTrait()).addTrait(DefaultTrait(Node.from("default")))
                traits.forEach { member.addTrait(it) }
            }
        traits.forEach { structureBuilder.addTrait(it) }
        val structure = structureBuilder.build()
        val original = Model.builder().addShapes(target, structure).build()
        val prepared = CodegenModel.prepare(original)
        for (shape in listOf(target, structure, structure.getMember("Value").orElseThrow())) {
            val normalized = prepared.expectShape(shape.id)
            assertEquals(shape.allTraits - traits.map { it.toShapeId() }.toSet(), normalized.allTraits)
            assertEquals(shape.allTraits, original.expectShape(shape.id).allTraits)
        }
    }

    @Test
    fun `preparation leaves the original dataplane model unchanged`() {
        val original = source()
        val snapshot = original.toBuilder().build()
        val etag = TmModelProjection.id("PutObjectOutput").withMember("ETag")
        val prepared = CodegenModel.prepare(original)
        assertEquals(snapshot, original)
        assertTrue(original.expectShape(etag).hasTrait(HttpHeaderTrait::class.java))
        assertFalse(prepared.expectShape(etag).hasTrait(HttpHeaderTrait::class.java))
        assertFalse(original.expectShape(TmModelProjection.id("GetObjectRequest")).mixins.isEmpty())
    }

    @Test
    fun `preparation is idempotent`() {
        val prepared = CodegenModel.prepare(source())
        assertEquals(prepared, CodegenModel.prepare(prepared))
    }
}
