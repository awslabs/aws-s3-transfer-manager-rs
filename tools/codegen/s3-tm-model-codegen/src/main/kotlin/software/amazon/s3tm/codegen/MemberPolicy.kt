/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.s3tm.codegen.customizations.RequestIdExt
import software.amazon.smithy.codegen.core.Symbol
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.node.Node
import software.amazon.smithy.model.node.ObjectNode
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.SensitiveTrait
import software.amazon.smithy.rust.codegen.core.rustlang.RustType
import software.amazon.smithy.rust.codegen.core.rustlang.RustWriter
import software.amazon.smithy.rust.codegen.core.rustlang.Writable
import software.amazon.smithy.rust.codegen.core.rustlang.asArgument
import software.amazon.smithy.rust.codegen.core.rustlang.asDeref
import software.amazon.smithy.rust.codegen.core.rustlang.asRef
import software.amazon.smithy.rust.codegen.core.rustlang.documentShape
import software.amazon.smithy.rust.codegen.core.rustlang.deprecatedShape
import software.amazon.smithy.rust.codegen.core.rustlang.docs
import software.amazon.smithy.rust.codegen.core.rustlang.isCopy
import software.amazon.smithy.rust.codegen.core.rustlang.isDeref
import software.amazon.smithy.rust.codegen.core.rustlang.render
import software.amazon.smithy.rust.codegen.core.rustlang.rust
import software.amazon.smithy.rust.codegen.core.rustlang.rustBlock
import software.amazon.smithy.rust.codegen.core.rustlang.stripOuter
import software.amazon.smithy.rust.codegen.core.rustlang.writable
import software.amazon.smithy.rust.codegen.core.smithy.RustSymbolProvider
import software.amazon.smithy.rust.codegen.core.smithy.RuntimeType
import software.amazon.smithy.rust.codegen.core.smithy.generators.BuilderCustomization
import software.amazon.smithy.rust.codegen.core.smithy.generators.BuilderSection
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructureCustomization
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructureSection
import software.amazon.smithy.rust.codegen.core.smithy.makeOptional
import software.amazon.smithy.rust.codegen.core.smithy.mapRustType
import software.amazon.smithy.rust.codegen.core.smithy.rustType

/** A single member policy supplies both structure and builder customizations. */
class MemberPolicy(private val source: Model, private val sourceSymbols: RustSymbolProvider) {
    data class RuntimeMapping(val type: RuntimeType, val clone: Boolean, val partialEq: Boolean)
    enum class Construction { OPTIONAL, DEFAULT, REQUIRED }
    data class Field(
        val name: String,
        val coreSymbol: Symbol,
        val construction: Construction = Construction.OPTIONAL,
        val runtime: RuntimeMapping? = null,
        val visibility: String = "pub",
        val sourceMember: MemberShape? = null,
        val documentation: String = "",
        val accessor: String = name,
        val builderVisibility: String? = null,
        val debug: Boolean = true,
    ) {
        val valueSymbol: Symbol
            get() = if (construction == Construction.OPTIONAL) coreSymbol.makeOptional() else coreSymbol
        val builderSymbol: Symbol get() = coreSymbol.makeOptional()
        val builderGetterSymbol: Symbol
            get() = builderSymbol.mapRustType { RustType.Reference(null, it) }
        val argument get() = coreSymbol.rustType().asArgument("input")
        val gated: Boolean get() = runtime != null
        val methodVisibility: String get() = builderVisibility ?: if (visibility == "pub") "pub" else "pub(crate)"

        data class Accessor(val symbol: Symbol, val expression: String)

        /** Follow StructureGenerator's borrowed/copy accessors, including flattened vectors. */
        val valueAccessor: Accessor
            get() {
                val type = valueSymbol.rustType()
                val flatten = type is RustType.Option && type.member is RustType.Vec
                val returnType = when {
                    flatten -> type.stripOuter<RustType.Option>().asDeref().asRef()
                    type.isCopy() -> type
                    type is RustType.Option && type.member.isDeref() -> type.asDeref()
                    type.isDeref() -> type.asDeref().asRef()
                    else -> type.asRef()
                }
                val value = when {
                    type.isCopy() -> "self.$name"
                    type is RustType.Option && type.member.isDeref() -> "self.$name.as_deref()"
                    type is RustType.Option -> "self.$name.as_ref()"
                    type.isDeref() -> "::std::ops::Deref::deref(&self.$name)"
                    else -> "&self.$name"
                }
                return Accessor(
                    valueSymbol.mapRustType { returnType },
                    value + if (flatten) ".unwrap_or_default()" else "",
                )
            }
    }

    private val inputStream = RuntimeMapping(TmRuntimeTypes.inputStream, clone = false, partialEq = false)
    private val readAhead = RuntimeMapping(TmRuntimeTypes.readAhead, clone = true, partialEq = true)
    private val checksum = RuntimeMapping(TmRuntimeTypes.checksumStrategy, clone = true, partialEq = false)
    private val failedUpload = RuntimeMapping(TmRuntimeTypes.failedMultipartUploadPolicy, clone = true, partialEq = false)
    private val metrics = RuntimeMapping(TmRuntimeTypes.transferMetrics, clone = true, partialEq = false)

    private fun runtime(name: String, mapping: RuntimeMapping, construction: Construction, documentation: String) =
        Field(name, mapping.type.toSymbol(), construction, mapping, documentation = documentation)

    private fun modeled(member: MemberShape, name: String, visibility: String = "pub"): Field {
        val symbol = sourceSymbols.toSymbol(member)
        require(symbol.rustType() is RustType.Option) {
            "Custom modeled member ${member.id} changed nullability; review its construction policy"
        }
        return Field(
            name, symbol.mapRustType { it.stripOuter<RustType.Option>() },
            visibility = visibility, sourceMember = member,
        )
    }

    val fields: Map<ShapeId, List<Field>> = buildMap {
        put(TmModelProjection.id("GetObjectRequest"), listOf(
            runtime("read_ahead", readAhead, Construction.OPTIONAL,
                "How far this download may prefetch ahead of the consumer. `None` uses the " +
                    "client default from [`Config`](crate::config::Config); `Some` overrides it for this request."),
            // Discovery owns SDK part numbers; do not add a public setter for an unsupported transfer.
            modeled(
                source.expectShape(TmModelProjection.id("GetObjectRequest"), StructureShape::class.java)
                    .getMember("PartNumber").orElseThrow(),
                "part_number",
            ).copy(
                builderVisibility = "pub(crate)",
                documentation = "Caller-selected part downloads are not supported. Transfer discovery manages request part numbers.",
            ),
        ))
        val put = source.expectShape(TmModelProjection.id("PutObjectRequest"), StructureShape::class.java)
        val body = put.getMember("Body").orElseThrow()
        put(put.id, listOf(
            runtime("body", inputStream, Construction.DEFAULT, "Object data.").copy(sourceMember = body),
            runtime("checksum_strategy", checksum, Construction.OPTIONAL, "Checksum calculation or precalculated checksum policy."),
            runtime("failed_multipart_upload_policy", failedUpload, Construction.OPTIONAL, "How to handle a failed multipart upload."),
        ) + put.members().mapNotNull {
            when (it.memberName) {
                "SSEKMSKeyId" -> modeled(it, "sse_kms_key_id")
                "SSEKMSEncryptionContext" -> modeled(it, "sse_kms_encryption_context")
                else -> null
            }
        })
        val output = source.expectShape(TmModelProjection.tmId("UploadOutput"), StructureShape::class.java)
        put(output.id, listOf(
            runtime("metrics", metrics, Construction.REQUIRED, "Snapshot of transfer metrics at completion.")
                .copy(builderVisibility = "pub(crate)", debug = false),
        ) + output.members().mapNotNull {
            when (it.memberName) {
                "SSEKMSKeyId" -> modeled(it, "sse_kms_key_id")
                "SSEKMSEncryptionContext" -> modeled(it, "sse_kms_encryption_context")
                else -> null
            }
        })
        for (name in listOf("ObjectMetadata", "ChunkMetadata")) {
            val shape = source.expectShape(TmModelProjection.tmId(name), StructureShape::class.java)
            put(shape.id, shape.members().mapNotNull {
                when {
                    name == "ObjectMetadata" && it.memberName in setOf("ContentLength", "ContentRange") ->
                        modeled(it, sourceSymbols.toMemberName(it), "pub(crate)")
                    else -> null
                }
            } + RequestIdExt.identifiers.map {
                modeled(shape.getMember(it.member).orElseThrow(), "_${it.accessor}", "")
                    .copy(accessor = it.accessor)
            })
        }
    }

    /** Remove only customized members from the ordinary renderer; retain the original model for audit. */
    fun emissionModel(): Model {
        val builder = source.toBuilder()
        fields.forEach { (id, additions) ->
            val shape = source.expectShape(id, StructureShape::class.java).toBuilder()
            additions.mapNotNull { it.sourceMember }.forEach { shape.removeMember(it.memberName) }
            val emitted = shape.build()
            val names = emitted.members().map { sourceSymbols.toMemberName(it) } + additions.map { it.name }
            require(names.size == names.toSet().size) {
                "TM member customization collides with a modeled member on $id; review its mapping"
            }
            builder.addShape(emitted)
        }
        return builder.build()
    }

    fun report(projection: TmModelProjection.Result): ObjectNode {
        val custom = Node.objectNodeBuilder()
        fields.toSortedMap().forEach { (shape, additions) ->
            additions.sortedBy { it.name }.forEach { field ->
                val item = Node.objectNodeBuilder()
                    .withMember("rustType", field.valueSymbol.rustType().render())
                    .withMember("construction", field.construction.name.lowercase())
                    .withMember("visibility", field.visibility.ifEmpty { "private" })
                    .withMember("builderVisibility", field.methodVisibility)
                    .withMember("sensitive", sensitive(field))
                    .withMember("debug", field.debug)
                    .withMember("synthetic", field.sourceMember?.let { projection.memberSources[it.id]?.isEmpty() } == true)
                    .withMember("sources", Node.fromStrings(
                        field.sourceMember?.let { projection.memberSources[it.id] ?: listOf(it.id) }
                            .orEmpty().map { it.toString() },
                    ))
                field.runtime?.let {
                    item.withMember("ownership", "tm").withMember("cfg", "not(s3_tm_out_of_tree)")
                        .withMember("clone", it.clone).withMember("partialEq", it.partialEq)
                }
                custom.withMember("$shape\$${field.name}", item.build())
            }
        }
        val excluded = Node.objectNodeBuilder()
        projection.excludedMembers.toSortedMap().forEach { (id, reason) -> excluded.withMember(id.toString(), reason) }
        val requestIds = Node.objectNodeBuilder()
        RequestIdExt.containers.forEach { id ->
            RequestIdExt.identifiers.forEach {
                requestIds.withMember(id.withMember(it.member).toString(), it.header)
            }
        }
        return Node.objectNodeBuilder().withMember("schemaVersion", 1)
            .withMember("defaultPolicy", "Generate modeled members and their reachable values.")
            .withMember("valueCustomizations", Node.objectNodeBuilder()
                .withMember("S3Expires", "UploadInput.expires is DateTime; metadata.expires_string preserves raw strings.")
                .withMember("S3Optionality", "Remove boolean/numeric defaults; preserve omission of service-defaulted inputs.")
                .withMember("RequestIdExt", requestIds.build()).build())
            .withMember("customizedMembers", custom.build()).withMember("excludedMembers", excluded.build()).build()
    }

    private fun RustWriter.condition(field: Field) {
        if (field.gated) rust("##[cfg(not(s3_tm_out_of_tree))]")
    }

    internal fun RustWriter.fieldDocs(field: Field) {
        if (field.sourceMember != null) {
            documentShape(field.sourceMember, source)
            deprecatedShape(field.sourceMember)
            if (field.runtime == null && field.documentation.isNotEmpty()) docs(field.documentation)
        } else {
            docs(field.documentation)
        }
    }

    private fun sensitive(field: Field): Boolean = field.sourceMember?.let {
        it.hasTrait(SensitiveTrait::class.java) || source.expectShape(it.target).hasTrait(SensitiveTrait::class.java)
    } ?: false

    private fun RustWriter.debug(field: Field, formatter: String, shape: StructureShape) {
        if (!field.debug) return
        condition(field)
        val value = if (sensitive(field) || shape.hasTrait(SensitiveTrait::class.java)) {
            "\"*** Sensitive Data Redacted ***\""
        } else "self.${field.name}"
        rust("$formatter.field(\"${field.name}\", &$value);")
    }

    fun structureCustomization() = object : StructureCustomization() {
        override fun section(section: StructureSection): Writable = writable {
            val additions = fields[section.shape.id].orEmpty()
            when (section) {
                is StructureSection.AdditionalFields -> additions.forEach { field ->
                    fieldDocs(field)
                    condition(field)
                    rust("${field.visibility} ${field.name}: #T,", field.valueSymbol)
                }
                is StructureSection.AdditionalDebugFields -> additions.forEach { debug(it, section.formatterName, section.shape) }
                is StructureSection.AdditionalTraitImpls -> if (additions.isNotEmpty()) {
                    rustBlock("impl #T", sourceSymbols.toSymbol(section.shape)) {
                        additions.forEach { field ->
                            fieldDocs(field)
                            condition(field)
                            val getter = field.valueAccessor
                            val visibility = if (field.visibility == "pub(crate)") "pub(crate)" else "pub"
                            if (visibility == "pub(crate)") rust("##[allow(dead_code)]")
                            rustBlock("$visibility fn ${field.accessor}(&self) -> #T", getter.symbol) {
                                rust(getter.expression)
                            }
                        }
                        additions.filter { it.runtime == inputStream }.forEach { field ->
                            docs("Takes the upload stream, leaving an empty stream in its place.")
                            condition(field)
                            rust("##[allow(dead_code)]")
                            rustBlock("pub(crate) fn take_${field.name}(&mut self) -> #T", field.coreSymbol) {
                                rust("#T(&mut self.${field.name})", RuntimeType.std.resolve("mem::take"))
                            }
                        }
                    }
                    if (section.shape.id == TmModelProjection.id("GetObjectRequest")) {
                        val value = sourceSymbols.toSymbol(section.shape)
                        val builder = sourceSymbols.symbolForBuilder(section.shape)
                        rustBlock("impl #T<#T> for #T", RuntimeType.From, value, builder) {
                            rustBlock("fn from(value: #T) -> Self", value) {
                                rust("Self {")
                                section.shape.members().forEach {
                                    val name = sourceSymbols.toMemberName(it)
                                    rust("$name: value.$name,")
                                }
                                additions.forEach { field ->
                                    condition(field)
                                    rust("${field.name}: value.${field.name},")
                                }
                                rust("}")
                            }
                        }
                    }
                }
            }
        }
    }

    fun builderCustomization() = object : BuilderCustomization() {
        override fun section(section: BuilderSection): Writable = writable {
            val additions = fields[section.shape.id].orEmpty()
            when (section) {
                is BuilderSection.AdditionalFields -> additions.forEach { field ->
                    condition(field)
                    rust("pub(crate) ${field.name}: #T,", field.builderSymbol)
                }
                is BuilderSection.AdditionalDebugFields -> additions.forEach { debug(it, section.formatterName, section.shape) }
                is BuilderSection.AdditionalFieldsInBuild -> additions.forEach { field ->
                    condition(field)
                    val value = when (field.construction) {
                        Construction.OPTIONAL -> "self.${field.name}"
                        Construction.DEFAULT -> "self.${field.name}.unwrap_or_default()"
                        Construction.REQUIRED -> "self.${field.name}.expect(\"${field.name} must be set\")"
                    }
                    rust("${field.name}: $value,")
                }
                is BuilderSection.AdditionalMethods -> additions.forEach { field ->
                    val name = field.name.removePrefix("_")
                    fieldDocs(field)
                    condition(field)
                    format(field.coreSymbol)
                    val input = field.argument
                    if (field.methodVisibility != "pub") rust("##[allow(dead_code)]")
                    rustBlock("${field.methodVisibility} fn $name(mut self, ${input.argument}) -> Self") {
                        rust("self.${field.name} = #T(${input.value}); self", RuntimeType.Option.resolve("Some"))
                    }
                    fieldDocs(field)
                    condition(field)
                    if (field.methodVisibility != "pub") rust("##[allow(dead_code)]")
                    rustBlock("${field.methodVisibility} fn set_$name(mut self, input: #T) -> Self", field.builderSymbol) {
                        rust("self.${field.name} = input; self")
                    }
                    fieldDocs(field)
                    condition(field)
                    if (field.methodVisibility != "pub") rust("##[allow(dead_code)]")
                    rustBlock("${field.methodVisibility} fn get_$name(&self) -> #T", field.builderGetterSymbol) {
                        rust("&self.${field.name}")
                    }
                }
            }
        }
    }
}
