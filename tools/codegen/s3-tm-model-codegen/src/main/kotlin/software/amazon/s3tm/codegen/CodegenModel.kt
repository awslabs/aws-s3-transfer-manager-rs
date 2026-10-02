/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ShapeId
import software.amazon.smithy.model.transform.ModelTransformer

/** Prepares an in-memory model for value generation without changing the dataplane export. */
object CodegenModel {
    // Values have no serialization/deserialization; retain value-facing traits, not wire bindings.
    private val wireTraits = setOf(
        "httpHeader", "httpPrefixHeaders", "httpPayload", "httpLabel", "httpQuery",
        "httpQueryParams", "httpResponseCode", "xmlName", "xmlAttribute", "xmlFlattened",
        "xmlNamespace", "timestampFormat", "jsonName",
    ).map { ShapeId.from("smithy.api#$it") }.toSet()

    fun prepare(dataplane: Model): Model {
        val transformer = ModelTransformer.create()
        val flattened = transformer.flattenAndRemoveMixins(dataplane)
        return transformer.filterTraits(flattened) { _, trait -> trait.toShapeId() !in wireTraits }
    }
}
