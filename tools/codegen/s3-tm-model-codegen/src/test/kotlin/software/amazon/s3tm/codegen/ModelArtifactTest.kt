/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ModelArtifactTest {
    @TempDir
    lateinit var temporary: Path

    private fun summary(projectOnly: Boolean, description: String): String = ModelArtifact.summary(
        temporary.resolve("input.json"), description, temporary.resolve("smithy-build.json"),
        temporary.resolve("output"), temporary.resolve("build/model-codegen"), 112,
        listOf("HeadObject", "GetObject"), projectOnly,
    )

    @Test
    fun `generation summary identifies input projection and Rust destinations`() {
        val report = summary(false, "verified pin: example/models@revision")
        assertTrue(report.contains("Input model:           ${temporary.resolve("input.json")} (verified pin: example/models@revision)"))
        assertTrue(report.contains("Projection config:     ${temporary.resolve("smithy-build.json")}"))
        assertTrue(report.contains("Projection:            s3-tm-dataplane"))
        assertTrue(report.contains("Projection output dir: ${temporary.resolve("output/s3-tm-dataplane")}"))
        assertTrue(report.contains("Projected model:       ${temporary.resolve("output/s3-tm-dataplane/model/model.json")}"))
        assertTrue(report.contains("Operations:            112 input -> 2 retained"))
        assertTrue(report.contains("Retained operations:   GetObject, HeadObject"))
        assertTrue(report.contains("Codegen work dir:      ${temporary.resolve("build/model-codegen")}"))
        assertTrue(report.contains("Generated crate:       ${temporary.resolve("output/model")}"))
        assertTrue(report.contains("Generated module:      ${temporary.resolve("output/model/src/model/mod.rs")}"))
    }

    @Test
    fun `project-only summary distinguishes local input and does not claim Rust output`() {
        val report = summary(true, "local override")
        assertTrue(report.contains("${temporary.resolve("input.json")} (local override)"))
        assertTrue(report.contains("Mode:                  project-only (Rust generation skipped)"))
        assertFalse(report.contains("Generated crate:"))
        assertFalse(report.contains("Generated module:"))
        assertFalse(report.contains("Codegen work dir:"))
        assertFalse(report.contains("verified pin:"))
    }
}
