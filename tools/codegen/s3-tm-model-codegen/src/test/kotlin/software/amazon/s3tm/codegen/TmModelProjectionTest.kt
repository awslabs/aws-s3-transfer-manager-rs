/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.shapes.ServiceShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.model.traits.HttpHeaderTrait
import software.amazon.smithy.model.traits.SensitiveTrait
import software.amazon.smithy.model.transform.ModelTransformer

class TmModelProjectionTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `preserves input shape and member identity while naming it DownloadInput`() {
        val original = source()
        val projection = TmModelProjection.project(original)
        val input = projection.model.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
        assertEquals(original.expectShape(input.id, StructureShape::class.java).allMembers.keys, input.allMembers.keys)
        assertTrue(input.getMember("InheritedOption").isPresent)
        assertEquals(
            "DownloadInput",
            projection.model.expectShape(ModelLoader.serviceId, ServiceShape::class.java).rename[input.id],
        )
        assertTrue(projection.model.expectShape(TmModelProjection.id("CustomerKey")).hasTrait(SensitiveTrait::class.java))
    }

    @Test
    fun `metadata members retain GET and HEAD provenance and exclude body`() {
        val original = source()
        val projection = TmModelProjection.project(original)
        val metadata = projection.model.expectShape(
            ShapeId.from("s3.tm#ObjectMetadata"), StructureShape::class.java,
        )
        val get = original.expectShape(TmModelProjection.id("GetObjectOutput"), StructureShape::class.java)
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        assertEquals((get.allMembers.keys + head.allMembers.keys) - "Body", metadata.allMembers.keys)
        assertFalse(metadata.getMember("Body").isPresent)
        val member = metadata.getMember("ETag").orElseThrow()
        assertEquals(
            listOf(
                TmModelProjection.id("GetObjectOutput").withMember("ETag"),
                TmModelProjection.id("HeadObjectOutput").withMember("ETag"),
            ),
            projection.memberSources[member.id],
        )
        assertEquals(
            original.expectShape(TmModelProjection.id("GetObjectOutput").withMember("ETag")).allTraits,
            member.allTraits,
        )
        for ((name, operation) in listOf("GetOnly" to "GetObjectOutput", "HeadOnly" to "HeadObjectOutput")) {
            assertEquals(
                listOf(TmModelProjection.id(operation).withMember(name)),
                projection.memberSources[metadata.id.withMember(name)],
            )
        }
    }

    @Test
    fun `new input and one-sided metadata members generate their nested targets`() {
        val original = source()
        val details = StructureShape.builder().id(TmModelProjection.id("AdditionalDetails"))
            .addMember("Token", ShapeId.from("smithy.api#String")).build()
        val input = original.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalInput", details.id).build()
        val get = original.expectShape(TmModelProjection.id("GetObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalMetadata", details.id).build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(details, input, get).build())
        val metadata = projection.model.expectShape(ShapeId.from("s3.tm#ObjectMetadata"), StructureShape::class.java)
        assertEquals(details.id, metadata.getMember("AdditionalMetadata").orElseThrow().target)
        assertEquals(
            listOf(input.id.withMember("AdditionalInput")),
            projection.memberSources[input.id.withMember("AdditionalInput")],
        )
        assertEquals(
            listOf(get.id.withMember("AdditionalMetadata")),
            projection.memberSources[metadata.id.withMember("AdditionalMetadata")],
        )
        val generated = ModelGenerator.generate(projection, temporary.resolve("raw"), "1.6.3")
        ModelArtifact.assemble(generated, temporary.resolve("artifact"), projection)
        val rust = Files.walk(temporary.resolve("artifact/model/src")).use { files ->
            files.filter(Files::isRegularFile).map(Files::readString).toList().joinToString("\n")
        }
        assertTrue(rust.contains("pub struct AdditionalDetails"))
        assertTrue(rust.contains("pub additional_input:"))
        assertTrue(rust.contains("pub additional_metadata:"))
        assertTrue(rust.contains("pub token:"))
    }

    @Test
    fun `model member removals are reflected without a frozen field list`() {
        val original = source()
        val input = original.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
            .toBuilder().removeMember("Key").build()
        val updated = TmModelProjection.project(original.toBuilder().addShape(input).build())
        assertFalse(updated.model.expectShape(input.id, StructureShape::class.java).getMember("Key").isPresent)
    }

    @Test
    fun `GET HEAD documentation may differ without changing the metadata projection`() {
        val original = source()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        val member = head.getMember("ETag").orElseThrow().toBuilder()
            .addTrait(DocumentationTrait("HEAD-specific description")).build()
        val updated = original.toBuilder().addShape(head.toBuilder().addMember(member).build()).build()
        assertDoesNotThrow { TmModelProjection.project(updated) }
    }

    @Test
    fun `GET HEAD semantic member trait differences require explicit policy`() {
        val original = source()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        val member = head.getMember("ETag").orElseThrow().toBuilder()
            .addTrait(HttpHeaderTrait("different-header")).build()
        val updated = original.toBuilder().addShape(head.toBuilder().addMember(member).build()).build()
        assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(updated) }
    }

    @Test
    fun `GET HEAD target differences require explicit policy`() {
        val original = ModelTransformer.create().flattenAndRemoveMixins(source())
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        val member = head.getMember("ETag").orElseThrow().toBuilder()
            .target(ShapeId.from("smithy.api#Integer")).build()
        val updated = original.toBuilder().addShape(head.toBuilder().addMember(member).build()).build()
        val error = assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(updated) }
        assertTrue(error.message!!.contains("metadata differs for ETag"))
    }

    @Test
    fun `emits only the value closure with builders and helpers under model`() {
        val projection = TmModelProjection.project(source())
        val generated = ModelGenerator.generate(projection, temporary.resolve("raw"), "1.6.3")
        ModelArtifact.assemble(generated, temporary.resolve("artifact"), projection)
        val root = temporary.resolve("artifact/model")
        val files = Files.walk(root.resolve("src")).use { it.filter(Files::isRegularFile).toList() }
        val rust = files.joinToString("\n") { Files.readString(it) }
        assertTrue(rust.contains("pub struct DownloadInput"))
        assertTrue(rust.contains("pub struct ObjectMetadata"))
        assertFalse(rust.contains("pub struct GetObjectOutput"))
        assertFalse(rust.contains("pub struct GetObjectRequest"))
        assertTrue(rust.contains("pub inherited_option:"))
        assertTrue(rust.contains("pub get_only:"))
        assertTrue(rust.contains("pub head_only:"))
        assertFalse(rust.contains("pub body:"))
        assertFalse(rust.contains("aws_sdk_s3"))
        assertFalse(rust.contains("crate::error::"))
        assertTrue(files.all { it == root.resolve("src/lib.rs") || it.startsWith(root.resolve("src/model")) })
        val cargo = Files.readString(root.resolve("Cargo.toml"))
        assertTrue(cargo.contains("aws-smithy-types"))
        assertFalse(cargo.contains("aws-sdk"))
    }
}
