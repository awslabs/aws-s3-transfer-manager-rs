/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
@file:Suppress("DEPRECATION") // Exercise legacy enum-trait strings as well as native Smithy enums.

package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.smithy.model.shapes.EnumShape
import software.amazon.smithy.model.shapes.IntEnumShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.shapes.UnionShape
import software.amazon.smithy.model.traits.EnumDefinition
import software.amazon.smithy.model.traits.EnumTrait

class ModelGeneratorTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `emits native and legacy string enums reachable from modeled roots`() {
        val original = source()
        assertTrue(original.expectShape(TmModelProjection.id("ChecksumAlgorithm")) is EnumShape)
        val legacy = StringShape.builder().id(TmModelProjection.id("LegacyState"))
            .addTrait(EnumTrait.builder()
                .addEnum(EnumDefinition.builder().name("KNOWN").value("known").build()).build()).build()
        val owner = original.expectShape(TmModelProjection.id("Owner"), StructureShape::class.java)
            .toBuilder().addMember("State", legacy.id).build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(legacy, owner).build())
        val generated = ModelGenerator.generate(projection, temporary.resolve("raw"), "1.6.3")
        for ((file, name) in listOf("_checksum_algorithm.rs" to "ChecksumAlgorithm", "_legacy_state.rs" to "LegacyState")) {
            val rust = Files.readString(generated.baseDir.resolve("src/model/$file"))
            assertTrue(rust.contains("pub enum $name"))
            assertTrue(rust.contains("Unknown("))
        }
        assertTrue(Files.readString(generated.baseDir.resolve("src/model/_owner.rs")).contains("crate::model::LegacyState"))
    }

    @Test
    fun `unsupported unions and integer enums fail instead of disappearing from the closure`() {
        val shapes = listOf(
            UnionShape.builder().id(TmModelProjection.id("AdditionalUnion"))
                .addMember("Value", ShapeId.from("smithy.api#String")).build(),
            IntEnumShape.builder().id(TmModelProjection.id("AdditionalIntEnum")).addMember("KNOWN", 1).build(),
        )
        for (shape in shapes) {
            val original = source()
            val owner = original.expectShape(TmModelProjection.id("Owner"), StructureShape::class.java)
                .toBuilder().addMember("Additional", shape.id).build()
            val projection = TmModelProjection.project(original.toBuilder().addShapes(shape, owner).build())
            val error = assertThrows(IllegalStateException::class.java) {
                ModelGenerator.generate(projection, temporary.resolve(shape.id.name), "1.6.3")
            }
            assertTrue(error.message!!.contains("Unsupported value shape ${shape.id}"))
        }
    }
}
