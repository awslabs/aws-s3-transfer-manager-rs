// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

val smithyVersion = providers.gradleProperty("smithy.version").get()
val smithyRsVersion = providers.gradleProperty("smithy.rs.version").get()
val junitVersion = providers.gradleProperty("junit.version").get()
val junitPlatformVersion = providers.gradleProperty("junit.platform.version").get()
val repositoryRoot = projectDir.resolve("../../..").canonicalFile
val modelFile = providers.gradleProperty("modelFile")
    .map { repositoryRoot.resolve(it).canonicalFile.absolutePath }
val cacheFile = repositoryRoot.resolve("target/codegen/models/s3.json")
val projectionOutput = providers.gradleProperty("projectionOutput")
    .map { repositoryRoot.resolve(it).canonicalFile }
    .getOrElse(repositoryRoot.resolve("target/codegen/projections"))

dependencies {
    implementation("software.amazon.smithy:smithy-model:$smithyVersion")
    implementation("software.amazon.smithy:smithy-build:$smithyVersion")
    implementation("software.amazon.smithy:smithy-aws-traits:$smithyVersion")
    implementation("software.amazon.smithy:smithy-rules-engine:$smithyVersion")
    implementation("software.amazon.smithy:smithy-waiters:$smithyVersion")
    runtimeOnly("software.amazon.smithy:smithy-aws-endpoints:$smithyVersion")
    runtimeOnly("software.amazon.smithy:smithy-protocol-traits:$smithyVersion")
    runtimeOnly("software.amazon.smithy:smithy-cli:$smithyVersion")
    testImplementation("software.amazon.smithy:smithy-diff:$smithyVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
}

// Keep codegen dependencies separate from the model-loader classpath.
val smithyRsCodegen by configurations.creating
dependencies {
    smithyRsCodegen("software.amazon.smithy.rust:codegen-client:$smithyRsVersion")
    smithyRsCodegen("software.amazon.smithy.rust:codegen-core:$smithyRsVersion")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("software.amazon.s3tm.codegen.ModelLoaderKt")
}

tasks.test {
    useJUnitPlatform()
}

val testModelSource by tasks.registering(Exec::class) {
    workingDir(repositoryRoot)
    commandLine("python3", "-m", "unittest", "discover", "-s", "tools/scripts/tests", "-v")
}

tasks.test {
    dependsOn(testModelSource)
}

val fetchModel by tasks.registering(Exec::class) {
    workingDir(repositoryRoot)
    val command = mutableListOf("python3", "tools/scripts/fetch-model")
    if (modelFile.isPresent) {
        command.addAll(listOf("--model", modelFile.get()))
    }
    if (gradle.startParameter.isOffline) {
        command.add("--offline")
    }
    if (providers.gradleProperty("pinnedOnly").orNull == "true") {
        command.add("--pinned-only")
    }
    commandLine(command)
}

tasks.register<JavaExec>("codegen") {
    dependsOn(fetchModel, tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("software.amazon.s3tm.codegen.ModelProjectionKt")
    args(
        modelFile.orNull ?: cacheFile.absolutePath,
        projectDir.resolve("smithy-build.json").absolutePath,
        projectionOutput.absolutePath,
    )
}

tasks.register<JavaExec>("diffModels") {
    dependsOn(tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("software.amazon.smithy.cli.SmithyCli")
    doFirst {
        val oldModel = providers.gradleProperty("oldModel").orNull
        val newModel = providers.gradleProperty("newModel").orNull
        require(oldModel != null && newModel != null) { "Specify -PoldModel and -PnewModel" }
        args(
            "diff",
            "--no-config",
            "--severity", "NOTE",
            "--old", repositoryRoot.resolve(oldModel).canonicalPath,
            "--new", repositoryRoot.resolve(newModel).canonicalPath,
        )
    }
}

tasks.register("verifyCodegenDependencies") {
    doLast {
        smithyRsCodegen.resolve().sortedBy { it.name }.forEach { println(it.name) }
    }
}
