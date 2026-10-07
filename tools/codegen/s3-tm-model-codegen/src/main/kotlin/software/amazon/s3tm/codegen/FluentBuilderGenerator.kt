/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.rust.codegen.core.rustlang.Attribute
import software.amazon.smithy.rust.codegen.core.rustlang.RustModule
import software.amazon.smithy.rust.codegen.core.rustlang.RustType
import software.amazon.smithy.rust.codegen.core.rustlang.Visibility
import software.amazon.smithy.rust.codegen.core.rustlang.asArgument
import software.amazon.smithy.rust.codegen.core.rustlang.asOptional
import software.amazon.smithy.rust.codegen.core.rustlang.deprecatedShape
import software.amazon.smithy.rust.codegen.core.rustlang.documentShape
import software.amazon.smithy.rust.codegen.core.rustlang.docs
import software.amazon.smithy.rust.codegen.core.rustlang.render
import software.amazon.smithy.rust.codegen.core.rustlang.rust
import software.amazon.smithy.rust.codegen.core.rustlang.rustBlock
import software.amazon.smithy.rust.codegen.core.rustlang.stripOuter
import software.amazon.smithy.rust.codegen.core.smithy.RustCrate
import software.amazon.smithy.rust.codegen.core.smithy.RustSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.generators.getterName
import software.amazon.smithy.rust.codegen.core.smithy.generators.setterName
import software.amazon.smithy.rust.codegen.core.smithy.rustType

/** Delegates modeled field methods to value builders; execution remains in the TM wrapper. */
// TODO(codegen): Make upstream smithy-rs FluentBuilderGenerator expose reusable field delegation
// independently of its client/execution wrapper, so this renderer can use it directly.
object FluentBuilderGenerator {
    fun render(crate: RustCrate, model: Model, symbols: RustSymbolProvider, policy: MemberPolicy) {
        render(crate, model, symbols, policy, TmModelProjection.id("PutObjectRequest"), "upload", "Upload")
        render(crate, model, symbols, policy, TmModelProjection.id("GetObjectRequest"), "download", "Download")
    }

    private fun render(
        crate: RustCrate,
        model: Model,
        symbols: RustSymbolProvider,
        policy: MemberPolicy,
        input: ShapeId,
        operation: String,
        name: String,
    ) {
        val shape = model.expectShape(input, StructureShape::class.java)
        val module = RustModule.new(
            "_${operation}_fluent_builder", Visibility.PRIVATE, parent = ModelGenerator.modelModule,
            additionalAttributes = listOf(Attribute("cfg(not(s3_tm_out_of_tree))")),
            documentationOverride = "$name fluent field delegation.",
        )
        crate.withModule(module) {
            rustBlock("impl crate::operation::$operation::builders::${name}FluentBuilder") {
                shape.members().forEach { member ->
                    format(symbols.toSymbol(member))
                    val name = symbols.toMemberName(member)
                    val outer = symbols.toSymbol(member).rustType()
                    val core = outer.stripOuter<RustType.Option>()
                    fun docs() {
                        documentShape(member, model)
                        deprecatedShape(member)
                    }
                    when (core) {
                        is RustType.Vec -> {
                            val input = core.member.asArgument("input")
                            docs(
                                """
                                Appends an item to `${member.memberName}`.

                                To override the contents of this collection use [`${member.setterName()}`](Self::${member.setterName()}).
                                """,
                            )
                            docs()
                            rustBlock("pub fn $name(mut self, ${input.argument}) -> Self") {
                                rust("self.inner = self.inner.$name(${input.value}); self")
                            }
                        }
                        is RustType.HashMap -> {
                            val key = core.key.asArgument("k")
                            val value = core.member.asArgument("v")
                            docs(
                                """
                                Adds a key-value pair to `${member.memberName}`.

                                To override the contents of this collection use [`${member.setterName()}`](Self::${member.setterName()}).
                                """,
                            )
                            docs()
                            rustBlock("pub fn $name(mut self, ${key.argument}, ${value.argument}) -> Self") {
                                rust("self.inner = self.inner.$name(${key.value}, ${value.value}); self")
                            }
                        }
                        else -> {
                            val input = core.asArgument("input")
                            docs()
                            rustBlock("pub fn $name(mut self, ${input.argument}) -> Self") {
                                rust("self.inner = self.inner.$name(${input.value}); self")
                            }
                        }
                    }
                    val setter = member.setterName()
                    val input = outer.asOptional().asArgument("input")
                    docs()
                    rustBlock("pub fn $setter(mut self, ${input.argument}) -> Self") {
                        rust("self.inner = self.inner.$setter(${input.value}); self")
                    }
                    val getter = member.getterName()
                    docs()
                    rustBlock("pub fn $getter(&self) -> &${outer.asOptional().render(true)}") {
                        rust("self.inner.$getter()")
                    }
                }
                policy.fields[shape.id].orEmpty().filter { it.methodVisibility == "pub" }.forEach { field ->
                    val name = field.name.removePrefix("_")
                    fun docs() {
                        with(policy) { fieldDocs(field) }
                    }
                    val string = field.coreType in setOf("String", "::std::string::String")
                    val argument = if (string) "impl Into<String>" else field.coreType
                    val value = if (string) "input.into()" else "input"
                    docs()
                    rustBlock("pub fn $name(mut self, input: $argument) -> Self") {
                        rust("self.inner = self.inner.$name($value); self")
                    }
                    docs()
                    rustBlock("pub fn set_$name(mut self, input: Option<${field.coreType}>) -> Self") {
                        rust("self.inner = self.inner.set_$name(input); self")
                    }
                    docs()
                    rustBlock("pub fn get_$name(&self) -> &Option<${field.coreType}>") {
                        rust("self.inner.get_$name()")
                    }
                }
            }
        }
    }
}
