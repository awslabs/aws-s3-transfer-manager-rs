/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait
import software.amazon.smithy.rust.codegen.core.rustlang.Writable
import software.amazon.smithy.rust.codegen.core.rustlang.rust
import software.amazon.smithy.rust.codegen.core.rustlang.rustBlock
import software.amazon.smithy.rust.codegen.core.rustlang.writable
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructureCustomization
import software.amazon.smithy.rust.codegen.core.smithy.generators.StructureSection

/** Empty metadata has no observed service values, including boolean/numeric fields and request IDs. */
class DownloadMetadata : StructureCustomization() {
    companion object {
        /** Preserve service documentation, including descriptions inherited from member targets. */
        fun document(projection: TmModelProjection.Result): TmModelProjection.Result {
            val model = projection.model.toBuilder()
            for (name in listOf("ObjectMetadata", "ChunkMetadata")) {
                val shape = projection.model.expectShape(TmModelProjection.tmId(name), StructureShape::class.java)
                val builder = shape.toBuilder().addTrait(DocumentationTrait(
                    if (name == "ObjectMetadata") {
                        "Object metadata from the discovery GET or HEAD response. Some values are also " +
                            "present on the first chunk. Optional fields remain absent when discovery does not " +
                            "return them. Reported checksums describe the stored object; they do not by themselves " +
                            "indicate that the downloaded bytes were checksum-validated."
                    } else {
                        "Metadata from an individual GET response used to download a chunk. Optional fields " +
                            "remain absent when that response does not return them. Content length and range " +
                            "describe this response, which may cover only part of the object. Reported checksums " +
                            "do not by themselves indicate that the delivered bytes were checksum-validated."
                    },
                ))
                shape.members().forEach { member ->
                    val serviceDocs = member.getMemberTrait(projection.model, DocumentationTrait::class.java)
                        .map { it.value }.orElse("")
                    val sources = projection.memberSources[member.id].orEmpty().map { it.name }.toSet()
                    val availability = if (name == "ObjectMetadata") {
                        when (sources) {
                            setOf("GetObjectOutput") -> "<p>Available when discovery uses a GET response that returns this value.</p>"
                            setOf("HeadObjectOutput") -> "<p>Available when discovery uses a HEAD response that returns this value.</p>"
                            else -> ""
                        }
                    } else ""
                    if (serviceDocs.isNotEmpty() || availability.isNotEmpty()) {
                        builder.addMember(member.toBuilder().addTrait(
                            DocumentationTrait(listOf(serviceDocs, availability).filter { it.isNotEmpty() }.joinToString("\n")),
                        ).build())
                    }
                }
                model.addShape(builder.build())
            }
            return projection.copy(model = model.build())
        }
    }

    override fun section(section: StructureSection): Writable = writable {
        if (section is StructureSection.AdditionalTraitImpls &&
            section.shape.id in setOf(TmModelProjection.tmId("ObjectMetadata"), TmModelProjection.tmId("ChunkMetadata"))
        ) {
            rustBlock("impl ::std::default::Default for ${section.structName}") {
                rustBlock("fn default() -> Self") {
                    rust("Self::builder().build()")
                }
            }
        }
    }
}
