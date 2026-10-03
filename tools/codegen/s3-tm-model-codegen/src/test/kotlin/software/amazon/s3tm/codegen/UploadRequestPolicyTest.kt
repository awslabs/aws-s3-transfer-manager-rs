/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.sdkv1.SdkV1Generator
import software.amazon.s3tm.codegen.sdkv1.SdkV1Mapping
import software.amazon.s3tm.codegen.sdkv1.SdkV1Symbols
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.SensitiveTrait

class UploadRequestPolicyTest {
    @TempDir lateinit var temporary: Path
    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))
    private fun mapping(model: Model = source()): SdkV1Mapping {
        val projection = TmModelProjection.project(model)
        return SdkV1Mapping(projection, SdkV1Symbols(model, projection, "1.6.3"))
    }
    private fun changed(model: Model, name: String, update: (StructureShape.Builder) -> StructureShape.Builder): Model {
        val shape = CodegenModel.prepare(model).expectShape(TmModelProjection.id(name), StructureShape::class.java)
        return CodegenModel.prepare(model).toBuilder().addShape(update(shape.toBuilder()).build()).build()
    }

    @Test fun `PUT additions generate public fields and ordinary setters`() {
        val model = changed(source(), "PutObjectRequest") { it.addMember("NewOption", ShapeId.from("smithy.api#String")) }
        val mapping = mapping(model)
        val generated = SdkV1Generator(mapping).generate(temporary)
        assertTrue(Files.readString(generated.baseDir.resolve("convert.rs")).contains(".set_new_option("))
        assertTrue(mapping.projection.model.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
            .getMember("NewOption").isPresent)
    }

    @Test fun `new multipart fields require review even when matching PUT`() {
        val put = changed(source(), "PutObjectRequest") { it.addMember("NewOption", ShapeId.from("smithy.api#String")) }
        val model = changed(put, "UploadPartRequest") { it.addMember("NewOption", ShapeId.from("smithy.api#String")) }
        val error = assertThrows(IllegalArgumentException::class.java) { mapping(model) }
        assertTrue(error.message!!.contains("UploadPartRequest\$NewOption"))
        assertTrue(error.message!!.contains("PUT counterpart=com.amazonaws.s3#PutObjectRequest\$NewOption"))
    }

    @Test fun `all unreviewed fields are reported together`() {
        val model = changed(source(), "AbortMultipartUploadRequest") {
            it.addMember("NewOne", ShapeId.from("smithy.api#String")).addMember("NewTwo", ShapeId.from("smithy.api#String"))
        }
        val error = assertThrows(IllegalArgumentException::class.java) { mapping(model) }
        assertTrue(error.message!!.contains("NewOne"))
        assertTrue(error.message!!.contains("NewTwo"))
    }

    @Test fun `removed reviewed members fail instead of silently shrinking forwarding`() {
        val model = changed(source(), "UploadPartRequest") { it.removeMember("SSECustomerKey") }
        val error = assertThrows(IllegalArgumentException::class.java) { mapping(model) }
        assertTrue(error.message!!.contains("SSECustomerKey: reviewed policy member no longer exists"))
    }

    @Test fun `changed shared target requires review`() {
        val model = changed(source(), "UploadPartRequest") {
            it.addMember("SSECustomerKey", ShapeId.from("smithy.api#Integer"))
        }
        assertTrue(assertThrows(IllegalArgumentException::class.java) { mapping(model) }.message!!.contains("Incompatible PUT source"))
    }

    @Test fun `changed value traits require review`() {
        val model = CodegenModel.prepare(source())
        val input = model.expectShape(TmModelProjection.id("UploadPartRequest"), StructureShape::class.java)
        val member = input.getMember("RequestPayer").orElseThrow().toBuilder().addTrait(SensitiveTrait()).build()
        val updated = model.toBuilder().addShape(input.toBuilder().addMember(member).build()).build()
        assertTrue(assertThrows(IllegalArgumentException::class.java) { mapping(updated) }.message!!.contains("Incompatible PUT source"))
    }

    @Test fun `part copying excludes whole object digest and runtime fields`() {
        val mapping = mapping()
        val part = mapping.requests.single { it.operation.id.name == "UploadPart" }
        assertEquals(UploadRequestPolicy.rules.getValue("UploadPart").forward, part.members.map { it.sdk.memberName }.toSet())
        val report = mapping.report()
        assertTrue(report.contains("whole-object MD5"))
        assertTrue(report.contains("Abort-specific conditional parameter"))
        val generated = SdkV1Generator(mapping).generate(temporary)
        val rust = Files.readString(generated.baseDir.resolve("convert.rs"))
        val partFunction = rust.substringAfter("fn copy_upload_input_fields_to_upload_part").substringBefore("\npub(crate)")
        for (name in listOf("content_md5", "content_length", "body", "part_number", "upload_id", "checksum")) {
            assertFalse(partFunction.contains(".set_$name("), name)
        }
        assertFalse(mapping.projection.model.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
            .getMember("IfMatchInitiatedTime").isPresent)
    }

    @Test fun `new checksum member requires exposure review`() {
        val model = changed(source(), "PutObjectRequest") {
            it.addMember("ChecksumNewAlgorithm", ShapeId.from("smithy.api#String"))
        }
        assertTrue(assertThrows(IllegalStateException::class.java) { mapping(model) }.message!!.contains("new checksum member"))
    }

    @Test fun `forward and override policies cannot overlap`() {
        assertThrows(IllegalArgumentException::class.java) {
            UploadRequestPolicy.Rules(setOf("Bucket"), mapOf("Bucket" to UploadRequestPolicy.Route.Omit("conflict")))
        }
    }
}
