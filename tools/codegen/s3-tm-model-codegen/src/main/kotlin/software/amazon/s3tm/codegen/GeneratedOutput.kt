/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

/** Replaces disposable generated artifacts while preserving files outside the generated layout. */
object GeneratedOutput {
    const val generator = "s3-tm-model-codegen"
    private const val legacyInventoryName = "generated-files.json"

    fun digest(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    fun inventory(root: Path): Map<String, String> =
        Files.walk(root).use { paths ->
            paths.peek { require(!Files.isSymbolicLink(it)) { "Refusing symlink candidate: $it" } }
                .filter { Files.isRegularFile(it, LinkOption.NOFOLLOW_LINKS) }
                .sorted().toList().associate { root.relativize(it).toString().replace('\\', '/') to digest(Files.readAllBytes(it)) }
        }

    private fun safePath(root: Path, relative: String): Path {
        val path = Path.of(relative)
        require(!path.isAbsolute && path.normalize() == path && path.nameCount > 0) {
            "Invalid generated file path: $relative"
        }
        val target = root.resolve(path).normalize()
        require(target.startsWith(root) && target != root) { "Generated file escapes output: $relative" }
        var current: Path? = target
        while (current != null) {
            require(!Files.isSymbolicLink(current)) { "Refusing symlink in generated output: $current" }
            if (current != target && Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                require(Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) { "Generated parent is not a directory: $current" }
            }
            current = current.parent
        }
        return target
    }

    private fun fixedFiles(raw: Boolean): Set<String> =
        if (raw) {
            setOf("Cargo.toml", "member-policy.json")
        } else {
            setOf(
                "${ModelProjection.name}/model/model.json",
                "model/Cargo.toml", "model/build.rs", "model/member-sources.json",
                "model/member-policy.json", "model/provenance.json", "model/dependencies.json",
                "sdk_v1/mapping.json",
            )
        }

    private fun allowedFile(name: String, raw: Boolean): Boolean =
        name.startsWith(if (raw) "src/" else "model/src/") ||
            !raw && name.startsWith("sdk_v1/") && name.endsWith(".rs") || name in fixedFiles(raw)

    private fun existingFiles(output: Path, projectOnly: Boolean, raw: Boolean): Set<String> {
        val files = mutableSetOf<String>()
        if (!projectOnly) {
            val roots = if (raw) listOf("src") else listOf("model/src", "sdk_v1")
            roots.forEach { root ->
                val source = safePath(output, root)
                if (Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
                    require(Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
                        "Generated source root is not a directory: $source"
                    }
                    Files.walk(source).use { paths ->
                        paths.forEach { file ->
                            val name = output.relativize(file).toString().replace('\\', '/')
                            safePath(output, name)
                            if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) files.add(name)
                        }
                    }
                }
            }
        }
        val fixed = if (projectOnly) setOf("${ModelProjection.name}/model/model.json") else fixedFiles(raw)
        // Retire the obsolete ledger by name, without trusting paths or hashes from its contents.
        (fixed + legacyInventoryName).forEach { name ->
            if (Files.exists(safePath(output, name), LinkOption.NOFOLLOW_LINKS)) files.add(name)
        }
        return files
    }

    private fun atomicWrite(path: Path, bytes: ByteArray) {
        Files.createDirectories(path.parent)
        val temporary = Files.createTempFile(path.parent, ".s3-tm-write-", ".tmp")
        try {
            Files.write(temporary, bytes)
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } finally {
            Files.deleteIfExists(temporary)
        }
    }

    fun publish(candidate: Path, destination: Path, projectOnly: Boolean = false, raw: Boolean = false) {
        val output = destination.toAbsolutePath().normalize()
        require(output.parent != null && output != Path.of(System.getProperty("user.home")).toAbsolutePath()) {
            "Refusing broad generated output root: $output"
        }
        require(!projectOnly || !raw) { "Project-only publication is not a raw codegen output" }
        val fresh = inventory(candidate)
        fresh.keys.forEach { name ->
            require(allowedFile(name, raw) && (!projectOnly || name == "${ModelProjection.name}/model/model.json")) {
                "Unexpected generated file: $name"
            }
        }
        val old = existingFiles(output, projectOnly, raw)
        val targets = (old + fresh.keys).associateWith { safePath(output, it) }
        // Check every path before replacing or deleting any artifact.
        targets.values.forEach { file ->
            if (Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
                require(Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) { "Generated target is not a file: $file" }
            }
        }
        fresh.keys.forEach { name ->
            val file = targets.getValue(name)
            val bytes = Files.readAllBytes(candidate.resolve(name))
            if (!Files.exists(file) || !Files.readAllBytes(file).contentEquals(bytes)) {
                atomicWrite(file, bytes)
            }
        }
        (old - fresh.keys).sorted().forEach { name ->
            val file = targets.getValue(name)
            if (Files.deleteIfExists(file)) println("Removed stale generated file: $file")
        }
    }

    /** Only used for the exact temporary directory returned by createTempDirectory. */
    fun removeTemporary(directory: Path) {
        Files.walk(directory).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
        }
    }
}
