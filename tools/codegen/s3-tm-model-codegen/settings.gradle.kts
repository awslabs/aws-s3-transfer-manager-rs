// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

pluginManagement {
    plugins {
        id("org.jetbrains.kotlin.jvm") version providers.gradleProperty("kotlin.version").get()
    }
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "s3-tm-model-codegen"
