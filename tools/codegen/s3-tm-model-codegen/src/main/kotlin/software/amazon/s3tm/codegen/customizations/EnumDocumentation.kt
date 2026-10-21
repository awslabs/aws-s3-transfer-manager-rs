/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.smithy.rust.codegen.client.smithy.generators.InfallibleEnumType
import software.amazon.smithy.rust.codegen.core.rustlang.RustModule
import software.amazon.smithy.rust.codegen.core.rustlang.docs
import software.amazon.smithy.rust.codegen.core.rustlang.writable
import software.amazon.smithy.rust.codegen.core.smithy.generators.EnumGeneratorContext
import software.amazon.smithy.rust.codegen.core.smithy.generators.EnumType

/** Retain smithy-rs enum behavior while documenting TM-owned value evolution. */
class EnumDocumentation(unknownVariantModule: RustModule) : EnumType() {
    private val delegate = InfallibleEnumType(unknownVariantModule)

    override fun implFromForStr(context: EnumGeneratorContext) = delegate.implFromForStr(context)
    override fun implFromStr(context: EnumGeneratorContext) = delegate.implFromStr(context)
    override fun implFromForStrForUnnamedEnum(context: EnumGeneratorContext) =
        delegate.implFromForStrForUnnamedEnum(context)
    override fun implFromStrForUnnamedEnum(context: EnumGeneratorContext) =
        delegate.implFromStrForUnnamedEnum(context)
    override fun additionalEnumMembers(context: EnumGeneratorContext) = delegate.additionalEnumMembers(context)
    override fun additionalAsStrMatchArms(context: EnumGeneratorContext) = delegate.additionalAsStrMatchArms(context)
    override fun additionalEnumAttributes(context: EnumGeneratorContext) = delegate.additionalEnumAttributes(context)
    override fun additionalEnumImpls(context: EnumGeneratorContext) = delegate.additionalEnumImpls(context)

    override fun additionalDocs(context: EnumGeneratorContext) = writable {
        docs(
            """
            When matching `${context.enumName}`, handle known variants and retain a wildcard
            arm for values introduced by S3 after this crate was generated.

            To recognize a value that is not yet a named variant, use `as_str()`:

            ```text
            match value {
                // Handle known variants here.
                other if other.as_str() == "NewFeature" => { /* ... */ },
                _ => { /* ... */ },
            }
            ```

            This continues to recognize `"NewFeature"` if a future version of this crate
            adds a named variant for it. Avoid matching `Unknown` directly: its inner
            value is opaque, and that arm would no longer match the new named variant.
            """.trimIndent(),
            trimStart = false,
        )
    }
}
