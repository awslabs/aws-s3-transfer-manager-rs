/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Path
import software.amazon.smithy.build.ProjectionResult
import software.amazon.smithy.build.SmithyBuild
import software.amazon.smithy.model.Model
import software.amazon.smithy.model.shapes.ServiceShape

object ModelProjection {
    const val name = "s3-tm-dataplane"

    fun build(model: Model, config: Path, output: Path): ProjectionResult {
        val result =
            SmithyBuild()
                .config(config)
                .model(model)
                .outputDirectory(output)
                .projectionFilter { it == name }
                .pluginFilter { it == "model" }
                .build()
                .getProjectionResult(name)
                .orElseThrow { IllegalStateException("Missing projection: $name") }
        check(!result.isBroken) { result.events.joinToString("\n") }
        result.model.expectShape(ModelLoader.serviceId, ServiceShape::class.java)
        return result
    }
}

fun main(args: Array<String>) {
    require(args.size == 3) { "Usage: ModelProjection <input-model> <build-config> <output-dir>" }
    val input = Path.of(args[0]).toAbsolutePath().normalize()
    val config = Path.of(args[1]).toAbsolutePath().normalize()
    val output = Path.of(args[2]).toAbsolutePath().normalize()
    val result = ModelProjection.build(ModelLoader.load(input), config, output)
    val manifest =
        result.getPluginManifest("model")
            .orElseThrow { IllegalStateException("Missing model artifact") }
    val artifact = manifest.baseDir.resolve("model.json")
    ModelLoader.load(artifact)
    println(artifact)
}
