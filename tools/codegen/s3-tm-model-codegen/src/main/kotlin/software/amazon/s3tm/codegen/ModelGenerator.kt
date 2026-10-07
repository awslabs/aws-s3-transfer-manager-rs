/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
@file:Suppress("DEPRECATION") // smithy-rs uses the EnumTrait bridge for Smithy 2 enums.

package software.amazon.s3tm.codegen

import software.amazon.s3tm.codegen.customizations.DownloadMetadata
import java.nio.file.Files
import java.nio.file.Path
import software.amazon.smithy.build.FileManifest
import software.amazon.smithy.codegen.core.Symbol
import software.amazon.smithy.model.neighbor.Walker
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.shapes.EnumShape
import software.amazon.smithy.model.shapes.OperationShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.shapes.UnionShape
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.ClientOptionalTrait
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
import software.amazon.smithy.rust.codegen.core.smithy.RustSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.RuntimeType
import software.amazon.smithy.rust.codegen.core.smithy.WrappingSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.expectRustMetadata
import software.amazon.smithy.rust.codegen.core.smithy.meta
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

    private fun stringEnum(shape: Shape): StringShape? = when {
        shape is EnumShape -> shape
        shape is StringShape && shape.hasTrait(EnumTrait::class.java) -> shape
        else -> null
    }

    private object Modules : ModuleProvider {
        override fun moduleForShape(context: ModuleProviderContext, shape: Shape) = modelModule
        override fun moduleForBuilder(context: ModuleProviderContext, shape: Shape, symbol: Symbol) = buildersModule
        override fun moduleForOperationError(context: ModuleProviderContext, operation: OperationShape): RustModule.LeafModule =
            error("Operation errors are not part of the value projection")
        override fun moduleForEventStreamError(context: ModuleProviderContext, eventStream: UnionShape): RustModule.LeafModule =
            error("Event streams require an explicit TM projection")
    }

    internal fun settings(source: Model, smithyTypesVersion: String) =
        ClientRustSettings.from(source, Node.parse(
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

    internal fun symbols(source: Model, settings: ClientRustSettings): RustSymbolProvider {
        val symbolConfig =
            RustSymbolProviderConfig(
                settings.runtimeConfig, settings.codegenConfig.renameExceptions,
                settings.codegenConfig.nullabilityCheckMode, Modules,
                nameBuilderFor = { "${it.name}Builder" },
            )
        return RustClientCodegenPlugin.baseSymbolProvider(
            settings, source, settings.getService(source), symbolConfig,
            CombinedClientCodegenDecorator(emptyList()),
        )
    }

    fun generate(projection: TmModelProjection.Result, output: Path, smithyTypesVersion: String): FileManifest {
        val source = projection.model
        val settings = settings(source, smithyTypesVersion)
        val policy = MemberPolicy(source, symbols(source, settings))
        val model = policy.emissionModel()
        val base = symbols(model, settings)
        val symbols = object : WrappingSymbolProvider(base) {
            override fun toSymbol(shape: Shape): Symbol {
                val symbol = super.toSymbol(shape)
                if (shape !is StructureShape) return symbol
                val mappings = policy.fields[shape.id].orEmpty().mapNotNull { it.runtime }
                val remove = buildList {
                    if (policy.fields[shape.id].orEmpty().isNotEmpty()) add(RuntimeType.Debug)
                    if (mappings.any { !it.clone }) add(RuntimeType.Clone)
                    if (mappings.any { !it.partialEq }) add(RuntimeType.PartialEq)
                }
                return symbol.toBuilder().meta(symbol.expectRustMetadata().withoutDerives(*remove.toTypedArray())).build()
            }
        }
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
            // smithy-rs enum formatters omit Formatter's lifetime.
            rust("##![allow(elided_lifetimes_in_paths)]")
            rust("##![forbid(unsafe_code)]")
        }
        val shapes = projection.roots.flatMap { Walker(source).walkShapes(source.expectShape(it)) }
            .distinctBy { it.id }.sortedBy { it.id }
            .filter {
                it is StructureShape || stringEnum(it) != null ||
                    it.isUnionShape || it.isIntEnumShape
            }.map { model.expectShape(it.id) }
        shapes.forEach { shape ->
            val enum = stringEnum(shape)
            val privateModule = RustModule.private("_${symbols.toSymbol(shape).name.toSnakeCase()}", parent = modelModule)
            when {
                shape is StructureShape -> {
                    crate.inPrivateModuleWithReexport(privateModule, symbols.toSymbol(shape)) {
                        StructureGenerator(
                            model, symbols, this, shape, listOf(policy.structureCustomization(), DownloadMetadata()), StructSettings(true),
                        ).render()
                        implBlock(symbols.toSymbol(shape)) {
                            BuilderGenerator.renderConvenienceMethod(this, symbols, shape)
                        }
                    }
                    crate.inPrivateModuleWithReexport(privateModule, symbols.symbolForBuilder(shape)) {
                        BuilderGenerator(model, symbols, shape, listOf(policy.builderCustomization())).render(this)
                    }
                }
                enum != null ->
                    crate.inPrivateModuleWithReexport(privateModule, symbols.toSymbol(shape)) {
                        EnumGenerator(model, symbols, enum, InfallibleEnumType(unknownModule), emptyList()).render(this)
                    }
                shape.isUnionShape || shape.isIntEnumShape ->
                    error("Unsupported value shape ${shape.id}: requires an explicit TM projection")
            }
        }
        FluentBuilderGenerator.render(crate, model, symbols, policy)
        crate.finalize(settings, model, mapOf("workspace" to emptyMap<String, Any>()), emptyList())
        // The client builder leaves optional @required fields for serializer validation.
        // TM validates them at construction while retaining their Option representation.
        val checked = listOf(
            TmModelProjection.id("GetObjectRequest"), TmModelProjection.id("PutObjectRequest"),
            TmModelProjection.tmId("UploadOutput"),
        )
        checked.forEach { id ->
            addCheckedBuilder(model.expectShape(id, StructureShape::class.java), symbols, policy, output, manifest)
        }
        manifest.writeFile("member-policy.json", Node.prettyPrintJson(policy.report(projection)) + "\n")
        return manifest
    }

    private fun addCheckedBuilder(
        input: StructureShape,
        symbols: software.amazon.smithy.rust.codegen.core.smithy.RustSymbolProvider,
        policy: MemberPolicy,
        output: Path,
        manifest: FileManifest,
    ) {
        val name = symbols.toSymbol(input).name.toSnakeCase()
        val inputFile = output.resolve("src/model/_$name.rs")
        val original = Files.readString(inputFile)
        val signature = "pub fn build(self)"
        check(original.indexOf(signature) >= 0 && original.indexOf(signature) == original.lastIndexOf(signature)) {
            "Expected one smithy-rs builder build method for ${input.id}"
        }
        val wrapper = RustWriter.forModule("crate::model::_$name")
        val runtimeConfig = symbols.config.runtimeConfig
        wrapper.implBlock(symbols.symbolForBuilder(input)) {
            docs("Consumes the builder, validating required fields.")
            rustTemplate(
                "pub fn build(self) -> #{Result}<#{Input}, #{BuildError}> {",
                *RuntimeType.preludeScope,
                "Input" to symbols.toSymbol(input),
                "BuildError" to runtimeConfig.operationBuildError(),
            )
            input.members().filter {
                it.isRequired && !it.hasTrait(DefaultTrait::class.java) && !it.hasTrait(ClientOptionalTrait::class.java)
            }.forEach { member ->
                val name = symbols.toMemberName(member)
                rustBlock("if self.$name.is_none()") {
                    rust(
                        "return Err(#T);",
                        OperationBuildError(runtimeConfig).missingField(name, "A required field was not set"),
                    )
                }
            }
            policy.fields[input.id].orEmpty().filter {
                it.construction == MemberPolicy.Construction.REQUIRED ||
                    (it.runtime == null && it.sourceMember?.isRequired == true &&
                        !it.sourceMember.hasTrait(DefaultTrait::class.java) &&
                        !it.sourceMember.hasTrait(ClientOptionalTrait::class.java))
            }.forEach { field ->
                if (field.gated) rust("##[cfg(not(s3_tm_out_of_tree))]")
                rustBlock("if self.${field.name}.is_none()") {
                    rust("return Err(#T);", OperationBuildError(runtimeConfig).missingField(field.name, "A required field was not set"))
                }
            }
            if (BuilderGenerator.hasFallibleBuilder(input, symbols)) {
                rust("self.build_unchecked()")
            } else {
                rust("Ok(self.build_unchecked())")
            }
            rust("}")
        }
        manifest.writeFile(
            "src/model/_$name.rs",
            original.replaceFirst(signature, "fn build_unchecked(self)") + "\n" + wrapper.toString(),
        )
    }
}
