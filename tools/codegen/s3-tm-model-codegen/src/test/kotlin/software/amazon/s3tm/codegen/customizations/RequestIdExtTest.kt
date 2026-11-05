/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.ModelGenerator
import software.amazon.s3tm.codegen.ModelLoader
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape

class RequestIdExtTest {
    @TempDir
    lateinit var temporary: Path

    private fun emptyProjection(): TmModelProjection.Result {
        val shapes = RequestIdExt.containers.map { StructureShape.builder().id(it).build() }
        return TmModelProjection.Result(Model.builder().addShapes(shapes).build(), RequestIdExt.containers, emptyMap())
    }

    @Test
    fun `both identifiers are modeled synthetic strings without mutating the original`() {
        val original = emptyProjection()
        val updated = RequestIdExt.transform(original)
        RequestIdExt.containers.forEach { id ->
            val shape = updated.model.expectShape(id, StructureShape::class.java)
            RequestIdExt.identifiers.forEach {
                val member = shape.getMember(it.member).orElseThrow()
                assertEquals(ShapeId.from("smithy.api#String"), member.target)
                assertEquals(emptyList<ShapeId>(), updated.memberSources[member.id])
                assertFalse(original.model.expectShape(id, StructureShape::class.java).getMember(it.member).isPresent)
            }
        }
    }

    @Test
    fun `upstream identifiers cannot be silently overwritten by synthetic members`() {
        for (identifier in RequestIdExt.identifiers) {
            val original = emptyProjection()
            val objectShape = original.model.expectShape(RequestIdExt.containers.first(), StructureShape::class.java)
                .toBuilder().addMember(identifier.member, ShapeId.from("smithy.api#String")).build()
            val error = assertThrows(IllegalArgumentException::class.java) {
                RequestIdExt.transform(original.copy(model = original.model.toBuilder().addShape(objectShape).build()))
            }
            assertTrue(error.message!!.contains("collides with the synthetic request ID"))
        }
    }

    @Test
    fun `report identifies synthetic fields and header mappings rather than invented upstream members`() {
        val source = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))
        val generated = ModelGenerator.generate(TmModelProjection.project(source), temporary.resolve("raw"), "1.6.3")
        val report = Node.parse(Files.readString(generated.baseDir.resolve("member-policy.json"))).expectObjectNode()
        val custom = report.expectObjectMember("customizedMembers")
        val headers = report.expectObjectMember("valueCustomizations").expectObjectMember("RequestIdExt")
        RequestIdExt.containers.forEach { id ->
            RequestIdExt.identifiers.forEach {
                val field = custom.expectObjectMember("$id\$_${it.accessor}")
                assertTrue(field.expectBooleanMember("synthetic").value)
                assertEquals("private", field.expectStringMember("visibility").value)
                assertEquals("pub(crate)", field.expectStringMember("builderVisibility").value)
                assertTrue(field.expectArrayMember("sources").elements.isEmpty())
                assertEquals(it.header, headers.expectStringMember(id.withMember(it.member).toString()).value)
            }
        }
        val rust = Files.readString(generated.baseDir.resolve("src/model/_object_metadata.rs"))
        assertTrue(rust.contains("pub fn request_id(&self)"))
        assertTrue(rust.contains("pub fn extended_request_id(&self)"))
        assertFalse(rust.contains("aws_sdk_s3"))
    }
}
