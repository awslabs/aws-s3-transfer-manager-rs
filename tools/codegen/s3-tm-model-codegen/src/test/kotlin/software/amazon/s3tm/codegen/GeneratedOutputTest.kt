/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class GeneratedOutputTest {
    @TempDir
    lateinit var temporary: Path

    private fun candidate(name: String, files: Map<String, String>): Path {
        val root = temporary.resolve(name)
        Files.createDirectories(root)
        files.forEach { (relative, content) ->
            val file = root.resolve(relative)
            Files.createDirectories(file.parent)
            Files.writeString(file, content)
        }
        return root
    }

    @Test
    fun `fresh inventory is stable and repeat publication changes nothing`() {
        val fresh = candidate("candidate", mapOf("model/src/model/mod.rs" to "generated", "model/Cargo.toml" to "crate"))
        val output = temporary.resolve("output")
        GeneratedOutput.publish(fresh, output)
        val before = GeneratedOutput.inventory(output)
        GeneratedOutput.publish(fresh, output)
        assertEquals(before, GeneratedOutput.inventory(output))
        assertFalse(Files.exists(output.resolve("generated-files.json")))
    }

    @Test
    fun `removes stale generated sources and preserves files outside the generated layout`() {
        val output = temporary.resolve("output")
        GeneratedOutput.publish(candidate("old", mapOf("model/src/model/old.rs" to "old")), output)
        val preserved = mapOf(
            "README.md" to "manual",
            "model/tests/contract.rs" to "test",
            "model/Cargo.lock" to "lock",
            "model/target/output" to "binary",
        )
        preserved.forEach { (name, content) ->
            val file = output.resolve(name)
            Files.createDirectories(file.parent)
            Files.writeString(file, content)
        }
        GeneratedOutput.publish(candidate("new", mapOf("model/src/model/new.rs" to "new")), output)
        assertFalse(Files.exists(output.resolve("model/src/model/old.rs")))
        preserved.forEach { (name, content) -> assertEquals(content, Files.readString(output.resolve(name))) }
        assertTrue(Files.exists(output.resolve("model/src/model/new.rs")))
    }

    @Test
    fun `locally modified scratch files are replaced by current generated output`() {
        val output = temporary.resolve("output")
        GeneratedOutput.publish(candidate("old", mapOf("model/src/a.rs" to "old a", "model/src/b.rs" to "old b")), output)
        Files.writeString(output.resolve("model/src/b.rs"), "manual b")
        GeneratedOutput.publish(candidate("new", mapOf("model/src/a.rs" to "new a", "model/src/b.rs" to "new b")), output)
        assertEquals("new a", Files.readString(output.resolve("model/src/a.rs")))
        assertEquals("new b", Files.readString(output.resolve("model/src/b.rs")))
    }

    @Test
    fun `locally modified stale files within the generated subtree are removed`() {
        val output = temporary.resolve("output")
        GeneratedOutput.publish(candidate("old", mapOf("model/src/stale.rs" to "old")), output)
        Files.writeString(output.resolve("model/src/stale.rs"), "manual")
        GeneratedOutput.publish(candidate("new", mapOf("model/src/other.rs" to "new")), output)
        assertFalse(Files.exists(output.resolve("model/src/stale.rs")))
        assertEquals("new", Files.readString(output.resolve("model/src/other.rs")))
    }

    @Test
    fun `pre-ledger output regenerates without adopting ownership or requiring a fresh directory`() {
        val output = candidate("output", mapOf("model/src/file.rs" to "old", "model/Cargo.toml" to "old manifest"))
        GeneratedOutput.publish(
            candidate("new", mapOf("model/src/file.rs" to "new", "model/Cargo.toml" to "new manifest")), output,
        )
        assertEquals("new", Files.readString(output.resolve("model/src/file.rs")))
        assertEquals("new manifest", Files.readString(output.resolve("model/Cargo.toml")))
        assertFalse(Files.exists(output.resolve("generated-files.json")))
    }

    @Test
    fun `project only retains all existing crate files`() {
        val modelPath = "${ModelProjection.name}/model/model.json"
        val output = temporary.resolve("output")
        GeneratedOutput.publish(candidate("old", mapOf(
            modelPath to "old model", "model/Cargo.toml" to "crate", "model/src/model/mod.rs" to "source",
        )), output)
        GeneratedOutput.publish(candidate("new", mapOf(modelPath to "new model")), output, projectOnly = true)
        assertEquals("crate", Files.readString(output.resolve("model/Cargo.toml")))
        assertEquals("source", Files.readString(output.resolve("model/src/model/mod.rs")))
        assertEquals("new model", Files.readString(output.resolve(modelPath)))
    }

    @Test
    fun `symlink ancestors are rejected without modifying external files`() {
        val external = candidate("external", mapOf("file" to "external"))
        val output = temporary.resolve("output")
        Files.createDirectories(output)
        Files.createSymbolicLink(output.resolve("model"), external)
        assertThrows(IllegalArgumentException::class.java) {
            GeneratedOutput.publish(candidate("new", mapOf("model/src/file.rs" to "new")), output)
        }
        assertEquals("external", Files.readString(external.resolve("file")))
        assertFalse(Files.exists(external.resolve("src/file.rs")))
    }

    @Test
    fun `obsolete ledger is removed without trusting its paths or hashes`() {
        val output = candidate("output", mapOf("README.md" to "manual"))
        val external = candidate("external", mapOf("file" to "external"))
        Files.writeString(output.resolve("generated-files.json"), """
            {"schemaVersion":1,"generator":"s3-tm-model-codegen","files":{"README.md":"invalid","../external/file":"invalid"}}
        """.trimIndent())
        GeneratedOutput.publish(candidate("new", mapOf("model/src/mod.rs" to "generated")), output)
        assertEquals("manual", Files.readString(output.resolve("README.md")))
        assertEquals("external", Files.readString(external.resolve("file")))
        assertEquals("generated", Files.readString(output.resolve("model/src/mod.rs")))
        assertFalse(Files.exists(output.resolve("generated-files.json")))
    }

    @Test
    fun `non directory parents fail before publishing other files`() {
        val output = candidate("output", mapOf("model/src" to "manual file"))
        val before = GeneratedOutput.inventory(output)
        assertThrows(IllegalArgumentException::class.java) {
            GeneratedOutput.publish(candidate("new", mapOf("model/Cargo.toml" to "crate", "model/src/lib.rs" to "generated")), output)
        }
        assertEquals(before, GeneratedOutput.inventory(output))
    }

    @Test
    fun `symlinks within the stale generated subtree fail before any replacement`() {
        val output = candidate("output", mapOf("model/src/a.rs" to "old"))
        val external = candidate("external", mapOf("file" to "external"))
        Files.createSymbolicLink(output.resolve("model/src/stale.rs"), external.resolve("file"))
        assertThrows(IllegalArgumentException::class.java) {
            GeneratedOutput.publish(candidate("new", mapOf("model/src/a.rs" to "new", "model/Cargo.toml" to "crate")), output)
        }
        assertEquals("old", Files.readString(output.resolve("model/src/a.rs")))
        assertFalse(Files.exists(output.resolve("model/Cargo.toml")))
        assertEquals("external", Files.readString(external.resolve("file")))
        assertTrue(Files.isSymbolicLink(output.resolve("model/src/stale.rs")))
    }

    @Test
    fun `candidate files outside the generated layout fail before any replacement`() {
        val output = candidate("output", mapOf("README.md" to "manual", "model/src/a.rs" to "old"))
        val before = GeneratedOutput.inventory(output)
        assertThrows(IllegalArgumentException::class.java) {
            GeneratedOutput.publish(candidate("new", mapOf("README.md" to "bad", "model/src/a.rs" to "new")), output)
        }
        assertEquals(before, GeneratedOutput.inventory(output))
    }

    @Test
    fun `raw output is regenerated within its own source and manifest boundary`() {
        val output = candidate("output", mapOf(
            "src/old.rs" to "old", "Cargo.toml" to "old manifest", "member-policy.json" to "old policy",
            "Cargo.lock" to "lock", "target/output" to "binary", "README.md" to "manual",
        ))
        GeneratedOutput.publish(candidate("new", mapOf(
            "src/new.rs" to "new", "Cargo.toml" to "new manifest", "member-policy.json" to "new policy",
        )), output, raw = true)
        assertFalse(Files.exists(output.resolve("src/old.rs")))
        assertEquals("new", Files.readString(output.resolve("src/new.rs")))
        assertEquals("new manifest", Files.readString(output.resolve("Cargo.toml")))
        assertEquals("new policy", Files.readString(output.resolve("member-policy.json")))
        assertEquals("lock", Files.readString(output.resolve("Cargo.lock")))
        assertEquals("binary", Files.readString(output.resolve("target/output")))
        assertEquals("manual", Files.readString(output.resolve("README.md")))
    }
}
