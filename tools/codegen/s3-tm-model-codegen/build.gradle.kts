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

val modelDiscovery by configurations.creating
configurations.implementation {
    extendsFrom(modelDiscovery)
}

dependencies {
    modelDiscovery("software.amazon.smithy:smithy-model:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-build:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-aws-traits:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-rules-engine:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-waiters:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-aws-endpoints:$smithyVersion")
    modelDiscovery("software.amazon.smithy:smithy-protocol-traits:$smithyVersion")
    implementation("software.amazon.smithy.rust:codegen-client:$smithyRsVersion")
    implementation("software.amazon.smithy.rust:codegen-core:$smithyRsVersion")
    runtimeOnly("software.amazon.smithy:smithy-cli:$smithyVersion")
    testImplementation("software.amazon.smithy:smithy-diff:$smithyVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("software.amazon.s3tm.codegen.ModelLoaderKt")
}

tasks.test {
    useJUnitPlatform()
    doFirst {
        systemProperty("s3tm.modelClasspath", modelDiscovery.asPath)
    }
}

tasks.withType<JavaExec>().configureEach {
    doFirst {
        systemProperty("s3tm.modelClasspath", modelDiscovery.asPath)
    }
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
    mainClass.set("software.amazon.s3tm.codegen.ModelArtifactKt")
    args(
        modelFile.orNull ?: cacheFile.absolutePath,
        projectDir.resolve("smithy-build.json").absolutePath,
        projectionOutput.absolutePath,
        layout.buildDirectory.dir("model-codegen").get().asFile.absolutePath,
        providers.gradleProperty("smithy.types.version").get(),
        providers.gradleProperty("projectOnly").getOrElse("false"),
        if (modelFile.isPresent) "local override" else
            "verified pin: ${providers.gradleProperty("model.repository").get()}@${providers.gradleProperty("model.revision").get()}",
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

val generateTestModel by tasks.registering(JavaExec::class) {
    dependsOn(tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("software.amazon.s3tm.codegen.ModelArtifactKt")
    args(
        projectDir.resolve("src/test/resources/s3-example-model.smithy").absolutePath,
        projectDir.resolve("smithy-build.json").absolutePath,
        layout.buildDirectory.dir("test-model").get().asFile.absolutePath,
        layout.buildDirectory.dir("test-model-codegen").get().asFile.absolutePath,
        providers.gradleProperty("smithy.types.version").get(),
        "false",
        "example model (test fixture)",
    )
    doLast {
        copy {
            from("src/test/resources/model_contract.rs")
            into(layout.buildDirectory.dir("test-model/model/tests"))
        }
    }
}

val testGeneratedModel by tasks.registering(Exec::class) {
    dependsOn(generateTestModel)
    workingDir(repositoryRoot)
    environment("CARGO_HOME", repositoryRoot.resolve("target/codegen/cargo-home").absolutePath)
    environment("CARGO_TARGET_DIR", repositoryRoot.resolve("target/codegen/cargo-target").absolutePath)
    val command = mutableListOf(
        "cargo", "test", "--quiet", "--manifest-path",
        layout.buildDirectory.file("test-model/model/Cargo.toml").get().asFile.absolutePath,
    )
    if (gradle.startParameter.isOffline) command.add("--offline")
    commandLine(command)
}

tasks.test {
    dependsOn(testGeneratedModel)
}
