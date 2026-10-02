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
import software.amazon.smithy.model.traits.RequiredTrait
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.node.Node
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
        assertEquals(
            (get.allMembers.keys + head.allMembers.keys) - setOf("Body", "Expires") +
                setOf("ExpiresString", "RequestId", "ExtendedRequestId"),
            metadata.allMembers.keys,
        )
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
        val upload = original.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalUploadInput", details.id).build()
        val create = original.expectShape(TmModelProjection.id("CreateMultipartUploadOutput"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalUploadMetadata", details.id).build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(details, input, get, upload, create).build())
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
        assertTrue(rust.contains("pub additional_upload_input:"))
        assertTrue(rust.contains("pub additional_upload_metadata:"))
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
    fun `GET HEAD wire bindings may differ without changing value meaning`() {
        val original = source()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        val member = head.getMember("ETag").orElseThrow().toBuilder()
            .addTrait(HttpHeaderTrait("different-header")).build()
        val updated = original.toBuilder().addShape(head.toBuilder().addMember(member).build()).build()
        val projection = TmModelProjection.project(updated)
        val aggregate = projection.model.expectShape(TmModelProjection.tmId("ObjectMetadata"), StructureShape::class.java)
        assertFalse(aggregate.getMember("ETag").orElseThrow().hasTrait(HttpHeaderTrait::class.java))
        assertFalse(projection.model.expectShape(member.id).hasTrait(HttpHeaderTrait::class.java))
        assertTrue(updated.expectShape(member.id).hasTrait(HttpHeaderTrait::class.java))
    }

    @Test
    fun `GET HEAD value semantic member trait differences require explicit policy`() {
        val original = source()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
        val member = head.getMember("ETag").orElseThrow().toBuilder().addTrait(SensitiveTrait()).build()
        val updated = original.toBuilder().addShape(head.toBuilder().addMember(member).build()).build()
        assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(updated) }
    }

    @Test
    fun `upload aggregate covers PUT create and complete without wire binding conflicts`() {
        val original = source()
        val projection = TmModelProjection.project(original)
        val output = projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
        val originals = listOf("PutObjectOutput", "CreateMultipartUploadOutput", "CompleteMultipartUploadOutput")
            .map { original.expectShape(TmModelProjection.id(it), StructureShape::class.java) }
        assertEquals(originals.flatMap { it.allMembers.keys }.toSet(), output.allMembers.keys)
        assertEquals(
            listOf(
                TmModelProjection.id("PutObjectOutput").withMember("ETag"),
                TmModelProjection.id("CompleteMultipartUploadOutput").withMember("ETag"),
            ),
            projection.memberSources[output.id.withMember("ETag")],
        )
        assertFalse(output.getMember("ETag").orElseThrow().hasTrait(HttpHeaderTrait::class.java))
        val etag = TmModelProjection.id("PutObjectOutput").withMember("ETag")
        assertFalse(projection.model.expectShape(etag).hasTrait(HttpHeaderTrait::class.java))
        assertTrue(original.expectShape(etag).hasTrait(HttpHeaderTrait::class.java))
        assertTrue(output.getMember("AbortDate").isPresent)
        assertTrue(output.getMember("Location").isPresent)
    }

    @Test
    fun `chunk metadata takes GET only and upload exclusions are recorded`() {
        val projection = TmModelProjection.project(source())
        val chunk = projection.model.expectShape(TmModelProjection.tmId("ChunkMetadata"), StructureShape::class.java)
        assertTrue(chunk.getMember("GetOnly").isPresent)
        assertFalse(chunk.getMember("HeadOnly").isPresent)
        assertFalse(chunk.getMember("Body").isPresent)
        val upload = projection.model.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        assertTrue(upload.getMember("AdditionalUpload").isPresent)
        assertTrue(upload.getMember("Body").isPresent)
        for (name in listOf("ChecksumAlgorithm", "ChecksumSHA256", "WriteOffsetBytes")) {
            assertFalse(upload.getMember(name).isPresent)
            assertTrue(projection.excludedMembers.containsKey(upload.id.withMember(name)))
        }
    }

    @Test
    fun `aggregate required and default conflicts fail even when wire bindings differ`() {
        for (trait in listOf(RequiredTrait(), DefaultTrait(Node.from("default")))) {
            val original = source()
            val complete = original.expectShape(TmModelProjection.id("CompleteMultipartUploadOutput"), StructureShape::class.java)
            val member = complete.getMember("ETag").orElseThrow().toBuilder().addTrait(trait).build()
            val updated = original.toBuilder().addShape(complete.toBuilder().addMember(member).build()).build()
            val error = assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(updated) }
            assertTrue(error.message!!.contains("UploadOutput metadata differs for ETag"))
        }
    }

    @Test
    fun `common structure sensitivity survives aggregation and differing sensitivity fails`() {
        val original = source()
        val outputs = listOf("GetObjectOutput", "HeadObjectOutput").map {
            original.expectShape(TmModelProjection.id(it), StructureShape::class.java)
                .toBuilder().addTrait(SensitiveTrait()).build()
        }
        val projection = TmModelProjection.project(original.toBuilder().addShapes(outputs).build())
        assertTrue(projection.model.expectShape(TmModelProjection.tmId("ObjectMetadata")).hasTrait(SensitiveTrait::class.java))
        assertTrue(projection.model.expectShape(TmModelProjection.tmId("ChunkMetadata")).hasTrait(SensitiveTrait::class.java))
        val mixed = original.toBuilder().addShape(outputs.first()).build()
        assertThrows(IllegalArgumentException::class.java) { TmModelProjection.project(mixed) }
    }

    @Test
    fun `TM additions cannot silently collide with an upstream member`() {
        val original = source()
        val input = original.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
            .toBuilder().addMember("ReadAhead", ShapeId.from("smithy.api#String")).build()
        val projection = TmModelProjection.project(original.toBuilder().addShape(input).build())
        val error = assertThrows(IllegalArgumentException::class.java) {
            ModelGenerator.generate(projection, temporary.resolve("raw"), "1.6.3")
        }
        assertTrue(error.message!!.contains("customization collides"))
    }

    @Test
    fun `required renamed input members receive builder validation while modeled defaults may be omitted`() {
        val original = source()
        val input = original.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        val required = input.getMember("SSEKMSKeyId").orElseThrow().toBuilder().addTrait(RequiredTrait()).build()
        val updated = original.toBuilder().addShape(input.toBuilder().addMember(required).build()).build()
        val projection = TmModelProjection.project(updated)
        val generated = ModelGenerator.generate(projection, temporary.resolve("required"), "1.6.3")
        assertTrue(Files.readString(generated.baseDir.resolve("src/model/_upload_input.rs"))
            .contains("if self.sse_kms_key_id.is_none()"))
        val defaulted = required.toBuilder().addTrait(DefaultTrait(Node.from("default"))).build()
        val withDefault = updated.toBuilder().addShape(input.toBuilder().addMember(defaulted).build()).build()
        val defaultGenerated = ModelGenerator.generate(TmModelProjection.project(withDefault), temporary.resolve("defaulted"), "1.6.3")
        assertFalse(Files.readString(defaultGenerated.baseDir.resolve("src/model/_upload_input.rs"))
            .contains("if self.sse_kms_key_id.is_none()"))
    }

    @Test
    fun `nested collection member provenance is retained`() {
        val projection = TmModelProjection.project(source())
        val member = TmModelProjection.id("Metadata").withMember("value")
        assertEquals(listOf(member), projection.memberSources[member])
    }

    @Test
    fun `fresh generation is byte identical across different work and output directories`() {
        val input = Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI())
        val first = temporary.resolve("first")
        val second = temporary.resolve("second")
        for ((output, work) in listOf(first to "work-a", second to "work-b")) {
            main(arrayOf(
                input.toString(), Path.of("smithy-build.json").toAbsolutePath().toString(),
                output.toString(), temporary.resolve(work).toString(), "1.6.3", "false", "example model",
            ))
        }
        assertEquals(GeneratedOutput.inventory(first), GeneratedOutput.inventory(second))
        val provenance = Files.readString(first.resolve("model/provenance.json"))
        assertFalse(provenance.contains(temporary.toString()))
        assertTrue(provenance.contains("generatorSha256"))
        assertTrue(provenance.contains("projectedModelSha256"))
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
        val metadataRust = Files.readString(root.resolve("src/model/_object_metadata.rs"))
        assertFalse(metadataRust.contains("pub body:"))
        assertTrue(rust.contains("pub struct UploadInput"))
        assertTrue(rust.contains("pub struct UploadOutput"))
        assertTrue(rust.contains("pub struct ChunkMetadata"))
        assertTrue(rust.contains("cfg(not(s3_tm_out_of_tree))"))
        assertTrue(rust.contains("pub body: crate::io::InputStream"))
        assertFalse(rust.contains("aws_sdk_s3"))
        assertFalse(rust.contains("crate::error::"))
        assertTrue(files.all { it == root.resolve("src/lib.rs") || it.startsWith(root.resolve("src/model")) })
        val cargo = Files.readString(root.resolve("Cargo.toml"))
        assertTrue(cargo.contains("aws-smithy-types"))
        assertFalse(cargo.contains("aws-sdk"))
    }
}
