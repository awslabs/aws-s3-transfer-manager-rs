/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.sdkv1.SdkV1Generator
import software.amazon.s3tm.codegen.sdkv1.SdkV1Mapping
import software.amazon.s3tm.codegen.sdkv1.SdkV1Symbols
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape

class SdkV1DownloadTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    private fun generate(model: Model = source()): Pair<SdkV1Mapping, Path> {
        val projection = TmModelProjection.project(model)
        val mapping = SdkV1Mapping(projection, SdkV1Symbols(model, projection, "1.6.3"))
        return mapping to SdkV1Generator(mapping).generate(temporary).baseDir
    }

    @Test
    fun `download interoperability emits only modeled metadata not streams or transfer results`() {
        val (_, output) = generate()
        val compat = Files.readString(output.resolve("compat.rs"))
        val compact = compat.replace(Regex("""\s+"""), "")
        for ((module, operation, target) in listOf(
            Triple("get_object", "GetObject", "ObjectMetadata"),
            Triple("head_object", "HeadObject", "ObjectMetadata"),
            Triple("get_object", "GetObject", "ChunkMetadata"),
        )) {
            val sdk = "::aws_sdk_s3::operation::$module::${operation}Output"
            for (argument in listOf(sdk, "&$sdk")) {
                assertTrue(compact.contains("implFrom<$argument>forcrate::model::$target{"), "$argument -> $target")
            }
            assertTrue(compat.contains("super::convert::${if (target == "ObjectMetadata") "object" else "chunk"}_metadata_from_$module"))
        }
        val head = "::aws_sdk_s3::operation::head_object::HeadObjectOutput"
        for (argument in listOf(head, "&$head")) {
            assertFalse(compact.contains("implFrom<$argument>forcrate::model::ChunkMetadata{"))
        }
        assertTrue(compat.contains("dropping the response body without reading it"))
        assertFalse(compat.contains("crate::operation::download::DownloadOutput"))
        assertFalse(compat.contains("GetObjectInput"))
        assertFalse(compat.contains("ByteStream"))
        val convert = Files.readString(output.resolve("convert.rs"))
        assertFalse(convert.contains("impl From<"))
        assertTrue(Regex("""#\[cfg\(feature = "sdk-v1"\)\]\s*mod compat;""")
            .containsMatchIn(Files.readString(output.resolve("mod.rs"))))
    }

    @Test
    fun `SDK identifier traits delegate to inherent modeled getters only in compat`() {
        val (_, output) = generate()
        val compact = Files.readString(output.resolve("compat.rs")).replace(Regex("""\s+"""), "")
        for (target in listOf("ObjectMetadata", "ChunkMetadata")) {
            for ((trait, accessor) in listOf("RequestId" to "request_id", "RequestIdExt" to "extended_request_id")) {
                assertTrue(compact.contains("impl::aws_sdk_s3::operation::$trait" + "forcrate::model::$target{"))
                assertTrue(compact.contains("crate::model::$target::$accessor(self)"))
            }
        }
        assertFalse(Files.readString(output.resolve("convert.rs")).contains("impl ::aws_sdk_s3::operation::RequestId"))
    }

    @Test
    fun `download response mapping covers all members except the separately owned body`() {
        val (mapping, output) = generate()
        val responses = mapping.responses.filter { it.output.id in setOf(
            TmModelProjection.tmId("ObjectMetadata"), TmModelProjection.tmId("ChunkMetadata"),
        ) }
        assertEquals(setOf("GetObject:ObjectMetadata", "HeadObject:ObjectMetadata", "GetObject:ChunkMetadata"),
            responses.map { "${it.operation.id.name}:${it.output.id.name}" }.toSet())
        val rust = Files.readString(output.resolve("convert.rs"))
        for (response in responses) {
            val sdk = mapping.symbols.sdkModel.expectShape(response.operation.output.orElseThrow(), StructureShape::class.java)
            assertEquals(sdk.allMembers.keys - "Body", response.members.map { it.sdk.memberName }.toSet())
            val function = "${if (response.output.id.name == "ObjectMetadata") "object" else "chunk"}_metadata_from_" +
                if (response.operation.id.name == "GetObject") "get_object" else "head_object"
            val body = rust.substringAfter("fn $function(").substringBefore("\npub(crate) fn ")
            for (member in response.members) {
                assertTrue(body.contains(".set_${mapping.symbols.tmField(member.tm)}("), member.tm.id.toString())
            }
            assertTrue(body.contains(".set_request_id(output.request_id().map(str::to_owned))"))
            assertTrue(body.contains(".set_extended_request_id(output.extended_request_id().map(str::to_owned))"))
            assertFalse(body.contains(".body("))
            assertFalse(body.contains(".set_body("))
        }
    }

    @Test
    fun `new GET metadata flows into both metadata values and SDK conversions`() {
        val original = source()
        val response = original.expectShape(TmModelProjection.id("GetObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalMetadata", ShapeId.from("smithy.api#String")).build()
        val (mapping, output) = generate(original.toBuilder().addShape(response).build())
        for (name in listOf("ObjectMetadata", "ChunkMetadata")) {
            val member = mapping.projection.model.expectShape(TmModelProjection.tmId(name), StructureShape::class.java)
                .getMember("AdditionalMetadata").orElseThrow()
            assertEquals(listOf(response.id.withMember("AdditionalMetadata")), mapping.projection.memberSources.getValue(member.id))
        }
        val rust = Files.readString(output.resolve("convert.rs"))
        assertEquals(2, Regex("""\.set_additional_metadata\(output\.additional_metadata\.to_owned\(\)\)""")
            .findAll(rust).count())
        val values = ModelGenerator.generate(mapping.projection, temporary.resolve("values"), "1.6.3")
        for (file in listOf("_object_metadata.rs", "_chunk_metadata.rs")) {
            assertTrue(Files.readString(values.baseDir.resolve("src/model/$file")).contains("pub additional_metadata:"))
        }
    }
}
