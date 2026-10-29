/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Path
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.loader.ModelDiscovery
import software.amazon.smithy.model.shapes.ServiceShape
import software.amazon.smithy.model.shapes.ShapeId

object ModelLoader {
    val serviceId: ShapeId = ShapeId.from("com.amazonaws.s3#AmazonS3")

    fun load(path: Path): Model {
        val assembler = Model.assembler()
        val libraries = System.getProperty("s3tm.modelClasspath")
        if (libraries == null) {
            assembler.discoverModels()
        } else {
            // Trait discovery is independent of the generators and their protocol-test models.
            URLClassLoader(
                libraries.split(File.pathSeparator).map { Path.of(it).toUri().toURL() }.toTypedArray(),
                null,
            ).use { loader -> ModelDiscovery.findModels(loader).forEach { assembler.addImport(it) } }
        }
        return assembler
            .addImport(path)
            .assemble()
            .unwrap()
            .also { it.expectShape(serviceId, ServiceShape::class.java) }
    }
}

fun main(args: Array<String>) {
    require(args.size == 1) { "Usage: ModelLoader <s3-smithy-json>" }
    val path = Path.of(args.single()).toAbsolutePath().normalize()
    val model = ModelLoader.load(path)
    val service = model.expectShape(ModelLoader.serviceId, ServiceShape::class.java)
    println("Validated ${service.id} (${service.operations.size} operations) from $path")
}
