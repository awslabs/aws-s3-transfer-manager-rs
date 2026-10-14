/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.sdkv1

import software.amazon.s3tm.codegen.CodegenModel
import software.amazon.s3tm.codegen.MemberPolicy
import software.amazon.s3tm.codegen.ModelGenerator
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.rust.codegen.core.rustlang.RustType
import software.amazon.smithy.rust.codegen.core.smithy.rustType
import software.amazon.smithy.rust.codegen.core.util.toSnakeCase

/** SDK-side names and nullability stay separate from TM member customization. */
class SdkV1Symbols(dataplane: Model, projection: TmModelProjection.Result, runtimeVersion: String) {
    val sdkModel = CodegenModel.prepare(dataplane)
    val tmModel = projection.model
    val sdk = ModelGenerator.symbols(sdkModel, ModelGenerator.settings(sdkModel, runtimeVersion))
    val tm = ModelGenerator.symbols(tmModel, ModelGenerator.settings(tmModel, runtimeVersion))
    val policy = MemberPolicy(tmModel, tm)

    fun tmName(shape: Shape): String = tm.toSymbol(shape).name
    fun functionName(shape: Shape): String = tmName(shape).toSnakeCase()
    fun tmField(member: MemberShape): String =
        policy.fields[member.container].orEmpty().firstOrNull { it.sourceMember?.id == member.id }?.name
            ?: tm.toMemberName(member)
    fun sdkField(member: MemberShape): String =
        if (member.memberName == "Expires" && member.container.name in setOf("GetObjectOutput", "HeadObjectOutput")) {
            "expires_string"
        } else sdk.toMemberName(member)
    fun tmOptional(member: MemberShape): Boolean = tm.toSymbol(member).rustType() is RustType.Option
    fun sdkOptional(member: MemberShape): Boolean = sdk.toSymbol(member).rustType() is RustType.Option
}
