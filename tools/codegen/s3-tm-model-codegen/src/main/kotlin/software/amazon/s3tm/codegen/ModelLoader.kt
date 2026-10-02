/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Path
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ServiceShape
import software.amazon.smithy.model.shapes.ShapeId

object ModelLoader {
    val serviceId: ShapeId = ShapeId.from("com.amazonaws.s3#AmazonS3")

    fun load(path: Path): Model =
        Model.assembler()
            .discoverModels()
            .addImport(path)
            .assemble()
            .unwrap()
            .also { it.expectShape(serviceId, ServiceShape::class.java) }
}

fun main(args: Array<String>) {
    require(args.size == 1) { "Usage: ModelLoader <s3-smithy-json>" }
    val path = Path.of(args.single()).toAbsolutePath().normalize()
    val model = ModelLoader.load(path)
    val service = model.expectShape(ModelLoader.serviceId, ServiceShape::class.java)
    println("Validated ${service.id} (${service.operations.size} operations) from $path")
}
