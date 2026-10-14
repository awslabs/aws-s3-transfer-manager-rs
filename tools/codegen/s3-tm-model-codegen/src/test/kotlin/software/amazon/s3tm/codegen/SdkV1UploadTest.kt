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

class SdkV1UploadTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    private fun generate(model: Model = source()): Pair<SdkV1Mapping, Path> {
        val projection = TmModelProjection.project(model)
        val mapping = SdkV1Mapping(projection, SdkV1Symbols(model, projection, "1.6.3"))
        return mapping to SdkV1Generator(mapping).generate(temporary).baseDir
    }

    @Test
    fun `upload response interoperability targets incomplete builders not transfer results`() {
        val (_, output) = generate()
        val compat = Files.readString(output.resolve("compat.rs"))
        val compact = compat.replace(Regex("""\s+"""), "")
        for ((module, operation) in listOf(
            "put_object" to "PutObject",
            "create_multipart_upload" to "CreateMultipartUpload",
            "complete_multipart_upload" to "CompleteMultipartUpload",
        )) {
            val sdk = "::aws_sdk_s3::operation::$module::${operation}Output"
            for (borrowed in listOf(false, true)) {
                val argument = if (borrowed) "&$sdk" else sdk
                assertTrue(compact.contains("implFrom<$argument>forcrate::model::builders::UploadOutputBuilder{"), argument)
                assertFalse(compact.contains("implFrom<$argument>forcrate::model::UploadOutput{"), argument)
            }
            assertTrue(compat.contains("super::convert::upload_output_from_$module"))
        }
        assertTrue(compat.contains("pub fn update_from_complete_mpu("))
        assertTrue(compat.contains("super::convert::update_upload_output_from_complete_multipart_upload"))
        assertFalse(compat.contains(".metrics("))
        assertFalse(compat.contains(".build()"))
        for (sdk in listOf(
            "::aws_sdk_s3::operation::get_object::GetObjectOutput",
            "::aws_sdk_s3::operation::head_object::HeadObjectOutput",
        )) {
            for (argument in listOf(sdk, "&$sdk")) {
                assertFalse(compact.contains("implFrom<$argument>forcrate::model::builders::UploadOutputBuilder{"), argument)
            }
        }
        assertFalse(compat.contains("impl From<crate::model::UploadOutput>"))
        val convert = Files.readString(output.resolve("convert.rs"))
        assertFalse(convert.contains("impl From<"))
        assertFalse(convert.contains("pub fn update_from_complete_mpu"))
        val root = Files.readString(output.resolve("mod.rs"))
        assertTrue(Regex("""#\[cfg\(feature = "sdk-v1"\)\]\s*mod compat;""").containsMatchIn(root))
    }

    @Test
    fun `every upload response member has correspondence without fabricating metrics`() {
        val (mapping, output) = generate()
        val responses = mapping.responses.filter { it.output.id == TmModelProjection.tmId("UploadOutput") }
        assertEquals(setOf("PutObject", "CreateMultipartUpload", "CompleteMultipartUpload"),
            responses.map { it.operation.id.name }.toSet())
        val rust = Files.readString(output.resolve("convert.rs"))
        for (response in responses) {
            val sdk = mapping.symbols.sdkModel.expectShape(response.operation.output.orElseThrow(), StructureShape::class.java)
            assertEquals(sdk.allMembers.keys, response.members.map { it.sdk.memberName }.toSet())
            val updateName = "update_upload_output_from_" + when (response.operation.id.name) {
                "PutObject" -> "put_object"
                "CreateMultipartUpload" -> "create_multipart_upload"
                else -> "complete_multipart_upload"
            }
            val update = rust.substringAfter("fn $updateName(").substringBefore("\npub(crate) fn ")
            for (member in response.members) {
                assertTrue(update.contains(".set_${mapping.symbols.tmField(member.tm)}("), member.tm.id.toString())
            }
            assertFalse(update.contains(".set_metrics("))
            assertFalse(update.contains(".metrics("))
            assertFalse(update.contains(".build()"))
        }
    }

    @Test
    fun `additive upload response members enter values and conversion generation together`() {
        val original = source()
        val output = original.expectShape(TmModelProjection.id("PutObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("AdditionalResult", ShapeId.from("smithy.api#String")).build()
        val (mapping, generated) = generate(original.toBuilder().addShape(output).build())
        val member = mapping.projection.model.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
            .getMember("AdditionalResult").orElseThrow()
        assertEquals(listOf(output.id.withMember("AdditionalResult")), mapping.projection.memberSources.getValue(member.id))
        val rust = Files.readString(generated.resolve("convert.rs"))
        assertTrue(rust.contains(".set_additional_result(output.additional_result.to_owned())"))
        val values = ModelGenerator.generate(mapping.projection, temporary.resolve("values"), "1.6.3")
        assertTrue(Files.readString(values.baseDir.resolve("src/model/_upload_output.rs"))
            .contains("pub additional_result:"))
    }
}
