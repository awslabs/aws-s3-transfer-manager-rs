/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.AbstractShapeBuilder
import software.amazon.smithy.model.shapes.MemberShape
import software.amazon.smithy.model.shapes.NumberShape
import software.amazon.smithy.model.shapes.Shape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.ClientOptionalTrait
import software.amazon.smithy.model.traits.DefaultTrait
import software.amazon.smithy.model.traits.InputTrait
import software.amazon.smithy.model.transform.ModelTransformer

/** Preserves absence instead of synthesizing false/zero for S3 boolean and numeric values. */
object S3Optionality {
    fun transform(model: Model): Model {
        val inputs = model.operationShapes.mapNotNull { it.input.orElse(null) }.toSet()
        val members = model.structureShapes.filter { it.id.namespace == "com.amazonaws.s3" }
            .flatMap { it.members() }.filter {
                val target = model.expectShape(it.target)
                target.isBooleanShape || target is NumberShape
            }.associateBy { it.id }
        val targets = members.values.map { it.target }.toSet()
        return ModelTransformer.create().mapShapes(model) { shape ->
            when {
                shape is MemberShape && shape.id in members -> {
                    val container = model.expectShape(shape.container, StructureShape::class.java)
                    val hadDefault = shape.hasTrait(DefaultTrait::class.java) ||
                        model.expectShape(shape.target).hasTrait(DefaultTrait::class.java)
                    val builder = shape.toBuilder().removeTrait(DefaultTrait.ID)
                    // Removing a service default must not make a previously omittable input mandatory.
                    if (hadDefault && shape.isRequired &&
                        (container.id in inputs || container.hasTrait(InputTrait::class.java))) {
                        builder.addTrait(ClientOptionalTrait())
                    }
                    builder.build()
                }
                shape.id in targets -> {
                    val builder: AbstractShapeBuilder<*, *> = Shape.shapeToBuilder(shape)
                    builder.removeTrait(DefaultTrait.ID).build()
                }
                else -> shape
            }
        }
    }
}
