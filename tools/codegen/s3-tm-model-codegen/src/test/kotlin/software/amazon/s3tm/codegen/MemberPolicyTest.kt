/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.smithy.codegen.core.Symbol
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.rust.codegen.core.rustlang.RustType
import software.amazon.smithy.rust.codegen.core.rustlang.RustWriter
import software.amazon.smithy.rust.codegen.core.rustlang.asArgument
import software.amazon.smithy.rust.codegen.core.rustlang.render
import software.amazon.smithy.rust.codegen.core.rustlang.rust
import software.amazon.smithy.rust.codegen.core.rustlang.stripOuter
import software.amazon.smithy.rust.codegen.core.smithy.RuntimeType
import software.amazon.smithy.rust.codegen.core.smithy.WrappingSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.makeOptional
import software.amazon.smithy.rust.codegen.core.smithy.mapRustType
import software.amazon.smithy.rust.codegen.core.smithy.rustType

class MemberPolicyTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    private fun symbol(type: RustType): Symbol =
        Symbol.builder().name(type.name).rustType(type).build()

    @Test
    fun `customized modeled fields and runtime fields use fully qualified type rendering`() {
        val generated = ModelGenerator.generate(TmModelProjection.project(source()), temporary, "1.6.3")
        fun rust(name: String) = Files.readString(generated.baseDir.resolve("src/model/_$name.rs"))
        val download = rust("download_input")
        assertTrue(download.contains("pub part_number: ::std::option::Option<i32>,"))
        assertTrue(download.contains("pub fn part_number(&self) -> ::std::option::Option<i32>"))
        assertTrue(download.contains("pub(crate) fn set_part_number(mut self, input: ::std::option::Option<i32>)"))
        assertTrue(download.contains("pub(crate) fn get_part_number(&self) -> &::std::option::Option<i32>"))
        assertTrue(download.contains(
            "impl ::std::convert::From<crate::model::DownloadInput> for crate::model::builders::DownloadInputBuilder",
        ))
        val runtimeFields = listOf(
            Triple("download_input", "read_ahead", "crate::types::ReadAhead"),
            Triple("upload_input", "body", "crate::io::InputStream"),
            Triple("upload_input", "checksum_strategy", "crate::operation::upload::ChecksumStrategy"),
            Triple("upload_input", "failed_multipart_upload_policy", "crate::types::FailedMultipartUploadPolicy"),
            Triple("upload_output", "metrics", "crate::types::TransferMetrics"),
        )
        for ((file, name, type) in runtimeFields) {
            val value = rust(file)
            assertTrue(value.contains("pub(crate) $name: ::std::option::Option<$type>,"), name)
            val visibility = if (name == "metrics") "pub(crate)" else "pub"
            assertTrue(value.contains("$visibility fn set_$name(mut self, input: ::std::option::Option<$type>)"), name)
            assertTrue(value.contains("$visibility fn get_$name(&self) -> &::std::option::Option<$type>"), name)
            if (file != "upload_output") {
                val fluent = rust(if (file == "download_input") "download_fluent_builder" else "upload_fluent_builder")
                assertTrue(fluent.contains("pub fn set_$name(mut self, input: ::std::option::Option<$type>)"), name)
                assertTrue(fluent.contains("pub fn get_$name(&self) -> &::std::option::Option<$type>"), name)
            }
        }
        assertTrue(rust("upload_input").contains(
            "pub fn sse_kms_key_id(mut self, input: impl ::std::convert::Into<::std::string::String>)",
        ))
        for (file in listOf("object_metadata", "chunk_metadata")) {
            val value = rust(file)
            assertTrue(value.contains("_request_id: ::std::option::Option<::std::string::String>,"))
            assertTrue(value.contains("pub fn request_id(&self) -> ::std::option::Option<&str>"))
            assertTrue(value.contains("pub fn extended_request_id(&self) -> ::std::option::Option<&str>"))
        }
    }

    @Test
    fun `construction distinguishes value storage from optional builder storage`() {
        val core = TmRuntimeTypes.inputStream.toSymbol()
        for (construction in MemberPolicy.Construction.entries) {
            val field = MemberPolicy.Field("body", core, construction)
            assertEquals(
                if (construction == MemberPolicy.Construction.OPTIONAL) RustType.Option(core.rustType()) else core.rustType(),
                field.valueSymbol.rustType(),
            )
            assertEquals(RustType.Option(core.rustType()), field.builderSymbol.rustType())
            assertEquals(RustType.Reference(null, RustType.Option(core.rustType())), field.builderGetterSymbol.rustType())
            assertEquals(core.rustType().asArgument("input"), field.argument)
        }
    }

    @Test
    fun `accessors and convenience arguments use structured type semantics`() {
        val cases = listOf(
            Triple(RustType.String, "::std::option::Option<&str>", "self.value.as_deref()"),
            Triple(RustType.Bool, "::std::option::Option<bool>", "self.value"),
            Triple(RustType.Integer(16), "::std::option::Option<i16>", "self.value"),
            Triple(RustType.Integer(32), "::std::option::Option<i32>", "self.value"),
            Triple(RustType.Integer(64), "::std::option::Option<i64>", "self.value"),
            Triple(RustType.Float(64), "::std::option::Option<f64>", "self.value"),
            Triple(TmRuntimeTypes.readAhead.toSymbol().rustType(), "::std::option::Option<&crate::types::ReadAhead>", "self.value.as_ref()"),
            Triple(RustType.Vec(RustType.String), "&[::std::string::String]", "self.value.as_deref().unwrap_or_default()"),
            Triple(
                RustType.HashMap(RustType.String, RustType.Vec(RustType.String)),
                "::std::option::Option<&::std::collections::HashMap::<::std::string::String, ::std::vec::Vec::<::std::string::String>>>",
                "self.value.as_ref()",
            ),
            Triple(
                RustType.Box(TmRuntimeTypes.readAhead.toSymbol().rustType()),
                "::std::option::Option<&crate::types::ReadAhead>",
                "self.value.as_deref()",
            ),
        )
        for ((type, getter, expression) in cases) {
            val field = MemberPolicy.Field("value", symbol(type))
            assertEquals(getter, field.valueAccessor.symbol.rustType().render(), type.toString())
            assertEquals(expression, field.valueAccessor.expression)
            assertEquals(type.asArgument("input"), field.argument)
        }
        val string = MemberPolicy.Field("value", symbol(RustType.String))
        assertEquals("input: impl ::std::convert::Into<::std::string::String>", string.argument.argument)
        assertEquals("input.into()", string.argument.value)
    }

    @Test
    fun `derived field symbols preserve dependencies without documentation rendering`() {
        val settings = ModelGenerator.settings(TmModelProjection.project(source()).model, "1.6.3")
        val core = RuntimeType.dateTime(settings.runtimeConfig).toSymbol()
        val original = core.makeOptional()
        val field = MemberPolicy.Field("value", original.mapRustType { it.stripOuter<RustType.Option>() })
        val dependencies = core.dependencies
        assertFalse(dependencies.isEmpty())
        for (derived in listOf(field.coreSymbol, field.valueSymbol, field.builderSymbol, field.builderGetterSymbol, field.valueAccessor.symbol)) {
            val writer = RustWriter.forModule("crate::model")
            writer.rust("type Test = #T;", derived)
            assertTrue(writer.dependencies.containsAll(dependencies), derived.rustType().render())
            assertTrue(writer.toString().contains("::aws_smithy_types::DateTime"))
        }
        val nested = MemberPolicy.Field("nested", core.mapRustType { RustType.Vec(RustType.Option(it)) })
        val writer = RustWriter.forModule("crate::model")
        writer.rust("type Nested = #T;", nested.builderGetterSymbol)
        assertTrue(writer.dependencies.containsAll(dependencies))
        assertTrue(writer.toString().contains("&::std::option::Option<::std::vec::Vec::<::std::option::Option<::aws_smithy_types::DateTime>>>"))
        assertTrue(TmRuntimeTypes.readAhead.toSymbol().dependencies.isEmpty())
    }

    @Test
    fun `customized modeled members follow target symbols rather than fixed scalar spellings`() {
        val original = source()
        val input = original.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
            .toBuilder().addMember("PartNumber", ShapeId.from("smithy.api#Timestamp")).build()
        val projection = TmModelProjection.project(original.toBuilder().addShape(input).build())
        val settings = ModelGenerator.settings(projection.model, "1.6.3")
        val policy = MemberPolicy(projection.model, ModelGenerator.symbols(projection.model, settings))
        val field = policy.fields.getValue(input.id).single { it.name == "part_number" }
        val writer = RustWriter.forModule("crate::model")
        writer.rust("type Target = #T;", field.valueAccessor.symbol)
        assertTrue(writer.dependencies.containsAll(RuntimeType.dateTime(settings.runtimeConfig).toSymbol().dependencies))
        val generated = ModelGenerator.generate(projection, temporary, "1.6.3")
        val rust = Files.readString(generated.baseDir.resolve("src/model/_download_input.rs"))
        assertTrue(rust.contains("pub part_number: ::std::option::Option<::aws_smithy_types::DateTime>"))
        assertTrue(rust.contains("pub fn part_number(&self) -> ::std::option::Option<&::aws_smithy_types::DateTime>"))
        assertTrue(rust.contains("pub(crate) fn part_number(mut self, input: ::aws_smithy_types::DateTime)"))
    }

    @Test
    fun `changed modeled nullability still requires a policy review`() {
        val projection = TmModelProjection.project(source())
        val base = ModelGenerator.symbols(projection.model, ModelGenerator.settings(projection.model, "1.6.3"))
        val symbols = object : WrappingSymbolProvider(base) {
            override fun toSymbol(shape: Shape): Symbol =
                super.toSymbol(shape).let {
                    if (shape is MemberShape && shape.id == TmModelProjection.id("GetObjectRequest").withMember("PartNumber")) {
                        it.mapRustType { type -> type.stripOuter<RustType.Option>() }
                    } else it
                }
        }
        val error = assertThrows(IllegalArgumentException::class.java) { MemberPolicy(projection.model, symbols) }
        assertTrue(error.message!!.contains("changed nullability"))
    }
}
