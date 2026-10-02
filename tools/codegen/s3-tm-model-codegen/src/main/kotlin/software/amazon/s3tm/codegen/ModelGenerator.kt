/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
@file:Suppress("DEPRECATION") // smithy-rs uses the EnumTrait bridge for Smithy 2 enums.

package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import software.amazon.smithy.build.FileManifest
import software.amazon.smithy.codegen.core.Symbol
import software.amazon.smithy.model.neighbor.Walker
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.shapes.UnionShape
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.EnumTrait
import software.amazon.smithy.rust.codegen.client.smithy.ClientRustSettings
import software.amazon.smithy.rust.codegen.client.smithy.RustClientCodegenPlugin
import software.amazon.smithy.rust.codegen.client.smithy.customize.CombinedClientCodegenDecorator
import software.amazon.smithy.rust.codegen.client.smithy.generators.InfallibleEnumType
import software.amazon.smithy.rust.codegen.core.rustlang.RustModule
import software.amazon.smithy.rust.codegen.core.rustlang.RustWriter
import software.amazon.smithy.rust.codegen.core.rustlang.Visibility
import software.amazon.smithy.rust.codegen.core.rustlang.docs
import software.amazon.smithy.rust.codegen.core.rustlang.implBlock
import software.amazon.smithy.rust.codegen.core.rustlang.rust
import software.amazon.smithy.rust.codegen.core.rustlang.rustBlock
import software.amazon.smithy.rust.codegen.core.rustlang.rustTemplate
import software.amazon.smithy.rust.codegen.core.rustlang.writable
import software.amazon.smithy.rust.codegen.core.smithy.ModuleDocProvider
import software.amazon.smithy.rust.codegen.core.smithy.ModuleProvider
import software.amazon.smithy.rust.codegen.core.smithy.ModuleProviderContext
import software.amazon.smithy.rust.codegen.core.smithy.RustCrate
import software.amazon.smithy.rust.codegen.core.smithy.RustSymbolProviderConfig
import software.amazon.smithy.rust.codegen.core.smithy.RuntimeType
import software.amazon.smithy.rust.codegen.core.smithy.customizations.AllowLintsCustomization
import software.amazon.smithy.rust.codegen.core.smithy.generators.BuilderGenerator
import software.amazon.smithy.rust.codegen.core.smithy.generators.EnumGenerator
import software.amazon.smithy.rust.codegen.core.smithy.generators.LibRsSection
import software.amazon.smithy.rust.codegen.core.smithy.generators.OperationBuildError
import software.amazon.smithy.rust.codegen.core.smithy.generators.operationBuildError
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructSettings
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructureGenerator
import software.amazon.smithy.rust.codegen.core.util.toSnakeCase

/** Uses smithy-rs value generators without invoking its service/protocol generators. */
object ModelGenerator {
    val modelModule = RustModule.public("model", documentationOverride = "Modeled S3 values.")
    val buildersModule = RustModule.public(
        "builders", parent = modelModule, documentationOverride = "Builders for modeled S3 values.",
    )
    private val unknownModule = RustModule.new(
        "sealed_enum_unknown", visibility = Visibility.PUBCRATE, parent = modelModule,
    )

    private object Modules : ModuleProvider {
        override fun moduleForShape(context: ModuleProviderContext, shape: Shape) = modelModule
        override fun moduleForBuilder(context: ModuleProviderContext, shape: Shape, symbol: Symbol) = buildersModule
        override fun moduleForOperationError(context: ModuleProviderContext, operation: OperationShape): RustModule.LeafModule =
            error("Operation errors are not part of the value projection")
        override fun moduleForEventStreamError(context: ModuleProviderContext, eventStream: UnionShape): RustModule.LeafModule =
            error("Event streams require an explicit TM projection")
    }

    fun generate(projection: TmModelProjection.Result, output: Path, smithyTypesVersion: String): FileManifest {
        val model = projection.model
        val settings = ClientRustSettings.from(model, Node.parse(
            """
            {
              "service": "${ModelLoader.serviceId}",
              "module": "s3-tm-model",
              "moduleVersion": "0.0.0",
              "moduleAuthors": ["Amazon Web Services"],
              "moduleDescription": "Modeled S3 values",
              "license": "Apache-2.0",
              "runtimeConfig": { "versions": { "aws-smithy-types": "=$smithyTypesVersion" } }
            }
            """,
        ).expectObjectNode())
        val symbols = RustClientCodegenPlugin.baseSymbolProvider(
            settings, model, settings.getService(model),
            RustSymbolProviderConfig(
                settings.runtimeConfig, settings.codegenConfig.renameExceptions,
                settings.codegenConfig.nullabilityCheckMode, Modules,
                nameBuilderFor = { "${it.name}Builder" },
            ),
            CombinedClientCodegenDecorator(emptyList()),
        )
        val manifest = FileManifest.create(output)
        val docs = object : ModuleDocProvider {
            override fun docsWriter(module: RustModule.LeafModule) =
                writable {
                    docs("Enum parsing errors.")
                }
        }
        val crate = RustCrate(manifest, symbols, settings.codegenConfig, docs)
        crate.withModule(modelModule) {
            AllowLintsCustomization().section(LibRsSection.Attributes)(this)
            rust("##![forbid(unsafe_code)]")
        }
        val shapes = projection.roots.flatMap { Walker(model).walkShapes(model.expectShape(it)) }
            .distinctBy { it.id }.sortedBy { it.id }
        shapes.forEach { shape ->
            val privateModule = RustModule.private("_${symbols.toSymbol(shape).name.toSnakeCase()}", parent = modelModule)
            when {
                shape is StructureShape -> {
                    crate.inPrivateModuleWithReexport(privateModule, symbols.toSymbol(shape)) {
                        StructureGenerator(model, symbols, this, shape, emptyList(), StructSettings(true)).render()
                        implBlock(symbols.toSymbol(shape)) {
                            BuilderGenerator.renderConvenienceMethod(this, symbols, shape)
                        }
                    }
                    crate.inPrivateModuleWithReexport(privateModule, symbols.symbolForBuilder(shape)) {
                        BuilderGenerator(model, symbols, shape, emptyList()).render(this)
                    }
                }
                shape is StringShape && shape.hasTrait(EnumTrait::class.java) ->
                    crate.inPrivateModuleWithReexport(privateModule, symbols.toSymbol(shape)) {
                        EnumGenerator(model, symbols, shape, InfallibleEnumType(unknownModule), emptyList()).render(this)
                    }
                shape.isUnionShape || shape.isIntEnumShape ->
                    error("Unsupported value shape ${shape.id}: requires an explicit TM projection")
            }
        }
        crate.finalize(settings, model, mapOf("workspace" to emptyMap<String, Any>()), emptyList())
        // The client builder leaves optional @required fields for serializer validation.
        // TM validates them at construction while retaining their Option representation.
        val input = model.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
        val inputFile = output.resolve("src/model/_download_input.rs")
        val original = Files.readString(inputFile)
        val signature = "pub fn build(self)"
        check(original.indexOf(signature) >= 0 && original.indexOf(signature) == original.lastIndexOf(signature)) {
            "Expected one smithy-rs builder build method for DownloadInput"
        }
        val wrapper = RustWriter.forModule("crate::model::_download_input")
        wrapper.implBlock(symbols.symbolForBuilder(input)) {
            docs("Consumes the builder, requiring all modeled required input fields to be set.")
            rustTemplate(
                "pub fn build(self) -> #{Result}<#{Input}, #{BuildError}> {",
                *RuntimeType.preludeScope,
                "Input" to symbols.toSymbol(input),
                "BuildError" to settings.runtimeConfig.operationBuildError(),
            )
            input.members().filter { it.isRequired && !it.hasTrait(DefaultTrait::class.java) }.forEach { member ->
                val name = symbols.toMemberName(member)
                rustBlock("if self.$name.is_none()") {
                    rust(
                        "return Err(#T);",
                        OperationBuildError(settings.runtimeConfig).missingField(name, "A required field was not set"),
                    )
                }
            }
            rust("self.build_unchecked()")
            rust("}")
        }
        manifest.writeFile(
            "src/model/_download_input.rs",
            original.replaceFirst(signature, "fn build_unchecked(self)") + "\n" + wrapper.toString(),
        )
        return manifest
    }
}
