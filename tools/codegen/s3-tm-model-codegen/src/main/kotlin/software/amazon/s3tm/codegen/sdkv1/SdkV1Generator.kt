/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.sdkv1

import java.nio.file.Path
import software.amazon.s3tm.codegen.customizations.RequestIdExt
import software.amazon.smithy.build.FileManifest
import software.amazon.smithy.model.shapes.ListShape
import software.amazon.smithy.model.shapes.MapShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.SparseTrait
import software.amazon.smithy.rust.codegen.core.smithy.generators.BuilderGenerator
import software.amazon.smithy.rust.codegen.core.util.toSnakeCase

/** Emits SDK bindings from one correspondence plan, independently of interoperability features. */
class SdkV1Generator(private val mapping: SdkV1Mapping) {
    private val symbols = mapping.symbols
    private val error = "::aws_smithy_types::error::operation::BuildError"

    private fun type(shape: Shape, toSdk: Boolean): String =
        if (toSdk) "::aws_sdk_s3::types::${symbols.sdk.toSymbol(symbols.sdkModel.expectShape(shape.id)).name}"
        else "crate::model::${symbols.tmName(shape)}"

    private fun function(shape: Shape, toSdk: Boolean): String =
        "${symbols.functionName(shape)}_${if (toSdk) "to" else "from"}_sdk"

    private fun fallible(shape: Shape, toSdk: Boolean, visiting: Set<software.amazon.smithy.model.shapes.ShapeId> = emptySet()): Boolean {
        require(shape.id !in visiting) { "Recursive SDK value ${shape.id} requires a recursive conversion policy" }
        val next = visiting + shape.id
        return when (shape) {
            is StructureShape -> {
                val sdkShape = symbols.sdkModel.expectShape(shape.id, StructureShape::class.java)
                val builder = if (toSdk) BuilderGenerator.hasFallibleBuilder(sdkShape, symbols.sdk)
                    else BuilderGenerator.hasFallibleBuilder(shape, symbols.tm)
                builder || mapping.valueMembers(shape).any {
                    toSdk && symbols.tmOptional(it.tm) && !symbols.sdkOptional(it.sdk) ||
                        !toSdk && symbols.sdkOptional(it.sdk) && !symbols.tmOptional(it.tm) ||
                        fallible(symbols.tmModel.expectShape(it.tm.target), toSdk, next)
                }
            }
            is ListShape -> fallible(symbols.tmModel.expectShape(shape.member.target), toSdk, next)
            is MapShape -> fallible(symbols.tmModel.expectShape(shape.value.target), toSdk, next)
            else -> false
        }
    }

    private fun convert(shape: Shape, value: String, toSdk: Boolean): String = when {
        SdkV1Mapping.isEnum(shape) || shape is StructureShape ->
            "${function(shape, toSdk)}($value)${if (fallible(shape, toSdk)) "?" else ""}"
        shape is ListShape -> {
            require(!shape.hasTrait(SparseTrait::class.java)) { "Sparse SDK list ${shape.id} requires an explicit conversion policy" }
            val member = symbols.tmModel.expectShape(shape.member.target)
            val expression = convert(member, "item", toSdk)
            if (fallible(member, toSdk)) "$value.iter().map(|item| Ok::<_, $error>($expression)).collect::<Result<_, _>>()?"
            else if (SdkV1Mapping.isEnum(member) || member is StructureShape) {
                "$value.iter().map(${function(member, toSdk)}).collect()"
            } else "$value.iter().map(|item| $expression).collect()"
        }
        shape is MapShape -> {
            require(!shape.hasTrait(SparseTrait::class.java) && symbols.tmModel.expectShape(shape.key.target).isStringShape) {
                "SDK map ${shape.id} requires a nonsparse string-key value policy"
            }
            val member = symbols.tmModel.expectShape(shape.value.target)
            val expression = convert(member, "item", toSdk)
            if (fallible(member, toSdk)) {
                "$value.iter().map(|(key, item)| Ok::<_, $error>((key.clone(), $expression))).collect::<Result<_, _>>()?"
            } else "$value.iter().map(|(key, item)| (key.clone(), $expression)).collect()"
        }
        shape.isStringShape || shape.isTimestampShape || shape.isBooleanShape ||
            shape.isByteShape || shape.isShortShape || shape.isIntegerShape || shape.isLongShape ||
            shape.isFloatShape || shape.isDoubleShape || shape.isBlobShape -> "$value.to_owned()"
        else -> error("Unsupported SDK value target ${shape.id} (${shape.type})")
    }

    private fun field(member: SdkV1Mapping.Member, variable: String, toSdk: Boolean, setter: Boolean = false): String {
        val sourceOptional = if (toSdk) symbols.tmOptional(member.tm) else symbols.sdkOptional(member.sdk)
        val targetOptional = setter || if (toSdk) symbols.sdkOptional(member.sdk) else symbols.tmOptional(member.tm)
        val sourceName = if (toSdk) symbols.tmField(member.tm) else symbols.sdkField(member.sdk)
        val source = "$variable.$sourceName"
        val shape = symbols.tmModel.expectShape(member.tm.target)
        return when {
            sourceOptional && targetOptional -> {
                val converted = convert(shape, "value", toSdk)
                if (fallible(shape, toSdk)) "$source.as_ref().map(|value| Ok::<_, $error>($converted)).transpose()?"
                else if (SdkV1Mapping.isEnum(shape) || shape is StructureShape) {
                    "$source.as_ref().map(${function(shape, toSdk)})"
                } else if (shape !is ListShape && shape !is MapShape) "$source.to_owned()"
                else "$source.as_ref().map(|value| $converted)"
            }
            sourceOptional -> {
                val required = "$source.as_ref().ok_or_else(|| $error::missing_field(\"$sourceName\", \"SDK value requires this field\"))?"
                convert(shape, "($required)", toSdk)
            }
            targetOptional -> "Some(${convert(shape, "(&$source)", toSdk)})"
            else -> convert(shape, "(&$source)", toSdk)
        }
    }

    private fun enums(): String = buildString {
        mapping.enums.forEach { shape ->
            for (toSdk in listOf(true, false)) {
                appendLine("pub(crate) fn ${function(shape, toSdk)}(value: &${type(shape, !toSdk)}) -> ${type(shape, toSdk)} {")
                appendLine("    ${type(shape, toSdk)}::from(value.as_str())")
                appendLine("}")
            }
        }
    }

    private fun values(): String = buildString {
        mapping.values.forEach { shape ->
            for (toSdk in listOf(true, false)) {
                val isFallible = fallible(shape, toSdk)
                val result = if (isFallible) "Result<${type(shape, toSdk)}, $error>" else type(shape, toSdk)
                appendLine("pub(crate) fn ${function(shape, toSdk)}(value: &${type(shape, !toSdk)}) -> $result {")
                appendLine("    let builder = ${type(shape, toSdk)}::builder()")
                mapping.valueMembers(shape).forEach { member ->
                    val targetName = if (toSdk) symbols.sdkField(member.sdk) else symbols.tmField(member.tm)
                    // set_ methods accept Option even when built storage is required/defaulted.
                    val converted = if (toSdk && symbols.tmOptional(member.tm) && !symbols.sdkOptional(member.sdk)) {
                        "Some(${field(member, "value", toSdk)})"
                    } else field(member, "value", toSdk, setter = true)
                    appendLine("        .set_$targetName($converted)")
                }
                appendLine("        ;")
                val builderFallible = if (toSdk) {
                    BuilderGenerator.hasFallibleBuilder(symbols.sdkModel.expectShape(shape.id, StructureShape::class.java), symbols.sdk)
                } else BuilderGenerator.hasFallibleBuilder(shape, symbols.tm)
                appendLine("    ${if (isFallible && !builderFallible) "Ok(builder.build())" else "builder.build()"}")
                appendLine("}")
            }
        }
    }

    private fun requests(): String = buildString {
        mapping.requests.forEach { request ->
            val name = request.operation.id.name.toSnakeCase()
            val sdkType = "::aws_sdk_s3::operation::$name::builders::${request.operation.id.name}FluentBuilder"
            appendLine("pub(crate) fn copy_${symbols.functionName(request.input)}_fields_to_$name(input: &${type(request.input, false)}, builder: $sdkType) -> $sdkType {")
            appendLine("    builder")
            request.members.forEach { member ->
                require(!fallible(symbols.tmModel.expectShape(member.tm.target), true)) {
                    "Request ${member.sdk.id} needs a fallible nested SDK conversion"
                }
                appendLine("        .set_${symbols.sdkField(member.sdk)}(${field(member, "input", true, setter = true)})")
            }
            appendLine("}")
        }
    }

    private fun responses(): String = buildString {
        appendLine("use ::aws_sdk_s3::operation::{RequestId, RequestIdExt};")
        mapping.responses.forEach { response ->
            val name = response.operation.id.name.toSnakeCase()
            val sdkType = "::aws_sdk_s3::operation::$name::${response.operation.id.name}Output"
            val outputName = symbols.tmName(response.output)
            val upload = outputName == "UploadOutput"
            val function = "${symbols.functionName(response.output)}_from_$name"
            val builderType = "crate::model::builders::${outputName}Builder"
            if (upload) {
                appendLine("pub(crate) fn $function(output: &$sdkType) -> $builderType {")
                appendLine("    update_$function(crate::model::$outputName::builder(), output)")
                appendLine("}")
                appendLine("pub(crate) fn update_$function(builder: $builderType, output: &$sdkType) -> $builderType {")
            } else {
                appendLine("pub(crate) fn $function(output: &$sdkType) -> crate::model::$outputName {")
                appendLine("    let builder = crate::model::$outputName::builder();")
            }
            appendLine("    builder")
            response.members.forEach { member ->
                require(!fallible(symbols.tmModel.expectShape(member.tm.target), false)) {
                    "Response ${member.sdk.id} needs a fallible nested TM conversion"
                }
                appendLine("        .set_${symbols.tmField(member.tm).removePrefix("_")}(${field(member, "output", false, setter = true)})")
            }
            if (!upload) {
                appendLine("        .set_request_id(output.request_id().map(str::to_owned))")
                appendLine("        .set_extended_request_id(output.extended_request_id().map(str::to_owned))")
                appendLine("        .build()")
            }
            appendLine("}")
        }
    }

    private fun compat(): String = buildString {
        (mapping.enums + mapping.values).forEach { shape ->
            for (toSdk in listOf(true, false)) {
                val isFallible = fallible(shape, toSdk)
                val source = type(shape, !toSdk)
                val target = type(shape, toSdk)
                for (borrowed in listOf(false, true)) {
                    val argument = if (borrowed) "&$source" else source
                    val call = "super::convert::${function(shape, toSdk)}(${if (borrowed) "value" else "&value"})"
                    if (isFallible) {
                        appendLine("impl TryFrom<$argument> for $target {")
                        appendLine("    type Error = $error;")
                        appendLine("    fn try_from(value: $argument) -> Result<Self, Self::Error> { $call }")
                    } else {
                        appendLine("impl From<$argument> for $target {")
                        appendLine("    fn from(value: $argument) -> Self { $call }")
                    }
                    appendLine("}")
                }
            }
        }
        mapping.responses.forEach { response ->
            val operation = response.operation.id.name
            val name = operation.toSnakeCase()
            val source = "::aws_sdk_s3::operation::$name::${operation}Output"
            val outputName = symbols.tmName(response.output)
            val target = if (outputName == "UploadOutput") "crate::model::builders::UploadOutputBuilder"
                else "crate::model::$outputName"
            val function = "${symbols.functionName(response.output)}_from_$name"
            for (borrowed in listOf(false, true)) {
                val argument = if (borrowed) "&$source" else source
                val value = if (borrowed) "value" else "&value"
                if (!borrowed && operation == "GetObject") {
                    appendLine("/// Extracts response metadata, dropping the response body without reading it.")
                }
                appendLine("impl From<$argument> for $target {")
                appendLine("    fn from(value: $argument) -> Self { super::convert::$function($value) }")
                appendLine("}")
            }
            if (outputName == "UploadOutput" && operation == "CompleteMultipartUpload") {
                appendLine("impl $target {")
                appendLine("    /// Updates completion response fields while retaining other transfer values.")
                appendLine("    pub fn update_from_complete_mpu(self, output: &$source) -> Self {")
                appendLine("        super::convert::update_$function(self, output)")
                appendLine("    }")
                appendLine("}")
            }
        }
        RequestIdExt.containers.forEach { id ->
            val target = "crate::model::${symbols.tmName(symbols.tmModel.expectShape(id))}"
            for ((trait, accessor) in listOf("RequestId" to "request_id", "RequestIdExt" to "extended_request_id")) {
                appendLine("impl ::aws_sdk_s3::operation::$trait for $target {")
                appendLine("    fn $accessor(&self) -> Option<&str> { $target::$accessor(self) }")
                appendLine("}")
            }
        }
    }

    fun generate(output: Path): FileManifest {
        val manifest = FileManifest.create(output.resolve("sdk_v1"))
        val modules = mapOf(
            "mod" to """
                #![allow(dead_code, unused_imports)]
                #![forbid(unsafe_code)]
                mod convert;
                #[cfg(feature = "sdk-v1")]
                mod compat;
                pub(crate) use convert::*;
            """.trimIndent(),
            "convert" to listOf(enums(), values(), requests(), responses()).joinToString("\n"),
            "compat" to compat(),
        )
        modules.forEach { (name, content) ->
            manifest.writeFile("$name.rs", "// Code generated by s3-tm-model-codegen. DO NOT EDIT.\n$content\n")
        }
        manifest.writeFile("mapping.json", mapping.report())
        val process = ProcessBuilder(listOf("rustfmt", "--edition", "2021") +
            modules.keys.map { manifest.baseDir.resolve("$it.rs").toString() }).inheritIO().start()
        check(process.waitFor() == 0) { "Formatting generated SDK v1 adapters failed" }
        return manifest
    }
}
