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
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.smithy.codegen.core.Symbol
import software.amazon.smithy.model.shapes.ListShape
import software.amazon.smithy.model.shapes.MapShape
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.rust.codegen.client.smithy.ClientCodegenContext
import software.amazon.smithy.rust.codegen.client.smithy.customize.CombinedClientCodegenDecorator
import software.amazon.smithy.rust.codegen.client.smithy.generators.client.FluentBuilderConfig
import software.amazon.smithy.rust.codegen.client.smithy.generators.client.FluentBuilderGenerator as UpstreamFluentBuilderGenerator
import software.amazon.smithy.rust.codegen.core.rustlang.RustWriter
import software.amazon.smithy.rust.codegen.core.rustlang.writable
import software.amazon.smithy.rust.codegen.core.smithy.WrappingSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.generators.getterName
import software.amazon.smithy.rust.codegen.core.smithy.generators.setterName

class FluentBuilderGeneratorTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @ParameterizedTest
    @ValueSource(strings = ["upload", "download"])
    fun `delegates every public input field and runtime customization with upstream getters`(operation: String) {
        val input = TmModelProjection.id(if (operation == "upload") "PutObjectRequest" else "GetObjectRequest")
        val projection = TmModelProjection.project(source())
        val settings = ModelGenerator.settings(projection.model, "1.6.3")
        val policy = MemberPolicy(projection.model, ModelGenerator.symbols(projection.model, settings))
        val generated = ModelGenerator.generate(projection, temporary, "1.6.3")
        val rust = Files.readString(generated.baseDir.resolve("src/model/_${operation}_fluent_builder.rs"))
        val symbols = ModelGenerator.symbols(policy.emissionModel(), settings)
        val names = policy.emissionModel().expectShape(input, StructureShape::class.java)
            .members().map { symbols.toMemberName(it) } +
            policy.fields.getValue(input).filter { it.methodVisibility == "pub" }.map { it.name }
        names.forEach {
            assertTrue(rust.contains("pub fn $it("), it)
            assertTrue(rust.contains("pub fn set_$it("), it)
            assertTrue(rust.contains("pub fn get_$it("), it)
            assertTrue(rust.contains("self.inner.get_$it()"), it)
        }
        assertTrue(rust.contains("-> &"))
        assertFalse(rust.contains("pub fn initiate"))
        assertFalse(rust.contains("aws_sdk_s3"))
        if (operation == "download") {
            assertFalse(rust.contains("pub fn part_number("))
            assertFalse(rust.contains("pub fn set_part_number("))
            assertFalse(rust.contains("pub fn get_part_number("))
            val value = Files.readString(generated.baseDir.resolve("src/model/_download_input.rs"))
            assertTrue(value.contains("pub part_number:"))
            assertTrue(value.contains("pub fn part_number(&self)"))
            assertTrue(value.contains("pub(crate) fn part_number("))
        }
        val root = Files.readString(generated.baseDir.resolve("src/model.rs"))
        assertTrue(Regex("""#\[cfg\(not\(s3_tm_out_of_tree\)\)\]\s*mod _${operation}_fluent_builder;""").containsMatchIn(root))
    }

    @ParameterizedTest
    @ValueSource(strings = ["upload", "download"])
    fun `new modeled input members automatically acquire fluent methods`(operation: String) {
        val original = source()
        val shape = original.expectShape(
            TmModelProjection.id(if (operation == "upload") "PutObjectRequest" else "GetObjectRequest"),
            StructureShape::class.java,
        )
            .toBuilder().addMember("FutureInput", ShapeId.from("smithy.api#String")).build()
        val generated = ModelGenerator.generate(
            TmModelProjection.project(original.toBuilder().addShape(shape).build()), temporary, "1.6.3",
        )
        val rust = Files.readString(generated.baseDir.resolve("src/model/_${operation}_fluent_builder.rs"))
        for (name in listOf("future_input", "set_future_input", "get_future_input")) {
            assertTrue(rust.contains("pub fn $name("))
        }
        assertTrue(rust.contains("self.inner.get_future_input()"))
    }

    @ParameterizedTest
    @ValueSource(strings = ["upload", "download"])
    fun `modeled field signatures and delegation match the pinned upstream fluent generator`(operation: String) {
        val original = source()
        val lists = ListShape.builder().id(TmModelProjection.id("Lists"))
            .member(TmModelProjection.id("StringList")).build()
        val map = MapShape.builder().id(TmModelProjection.id("ListMap"))
            .key(ShapeId.from("smithy.api#String")).value(TmModelProjection.id("StringList")).build()
        val input = original.expectShape(
            TmModelProjection.id(if (operation == "upload") "PutObjectRequest" else "GetObjectRequest"),
            StructureShape::class.java,
        )
            .toBuilder().addMember("FutureNumber", ShapeId.from("smithy.api#Integer"))
            .addMember("FutureLists", lists.id).addMember("FutureMap", map.id)
            .addMember("AdditionalLabels", TmModelProjection.id("StringList")).build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(lists, map, input).build())
        val settings = ModelGenerator.settings(projection.model, "1.6.3")
        val policy = MemberPolicy(projection.model, ModelGenerator.symbols(projection.model, settings))
        val model = policy.emissionModel()
        val symbols = object : WrappingSymbolProvider(ModelGenerator.symbols(model, settings)) {
            // Suppressed execution methods do not emit this type, but the upstream constructor resolves it.
            override fun symbolForOperationError(operation: OperationShape): Symbol =
                Symbol.builder().name("UnusedOperationError").namespace("crate::error", "::").build()
        }
        val context = ClientCodegenContext(
            model, symbols, null, settings.getService(model), ShapeId.from("aws.protocols#restXml"),
            settings, CombinedClientCodegenDecorator(emptyList()),
        )
        val upstream = RustWriter.forModule("model")
        UpstreamFluentBuilderGenerator(
            context, model.expectShape(
                TmModelProjection.id(if (operation == "upload") "PutObject" else "GetObject"), OperationShape::class.java,
            ),
            builderName = if (operation == "upload") "UploadFluentBuilder" else "DownloadFluentBuilder",
            config = object : FluentBuilderConfig {
                override fun documentBuilder() = writable {}
                override fun sendMethods() = writable {}
                override fun includePaginators() = false
                override fun includeConfigOverride() = false
            },
        ).render(upstream)
        val generated = ModelGenerator.generate(projection, temporary, "1.6.3")
        val ours = Files.readString(generated.baseDir.resolve("src/model/_${operation}_fluent_builder.rs"))
        val expected = upstream.toString()
        model.expectShape(input.id, StructureShape::class.java).members().forEach { member ->
            for (name in listOf(symbols.toMemberName(member), member.setterName(), member.getterName())) {
                assertEquals(method(expected, name), method(ours, name), name)
            }
        }
        for (guidance in listOf(
            "Appends an item to `AdditionalLabels`.",
            "Adds a key-value pair to `FutureMap`.",
            "To override the contents of this collection use",
        )) {
            assertTrue(expected.contains(guidance))
            assertTrue(ours.contains(guidance))
        }
    }

    private fun method(source: String, name: String): String {
        val start = Regex("""pub fn ${Regex.escape(name)}\(""").find(source)?.range?.first
            ?: error("Missing method $name")
        val body = source.indexOf('{', start)
        var depth = 1
        var end = body + 1
        while (depth > 0) {
            when (source[end++]) {
                '{' -> depth++
                '}' -> depth--
            }
        }
        return source.substring(start, end)
            .replace("::std::option::Option", "Option")
            .replace("::std::string::String", "String")
            .replace("::std::vec::Vec", "Vec")
            .replace("::std::convert::Into", "Into")
            .replace(Regex("""\s+"""), "")
    }
}
