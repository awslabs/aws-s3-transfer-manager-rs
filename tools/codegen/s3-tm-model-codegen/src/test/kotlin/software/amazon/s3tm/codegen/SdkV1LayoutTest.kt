/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.sdkv1.SdkV1Generator
import software.amazon.s3tm.codegen.sdkv1.SdkV1Mapping
import software.amazon.s3tm.codegen.sdkv1.SdkV1Symbols

class SdkV1LayoutTest {
    @TempDir
    lateinit var temporary: Path

    @Test
    fun `one unconditional conversion module serves internal adapters and gated interoperability`() {
        val model = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))
        val projection = TmModelProjection.project(model)
        val mapping = SdkV1Mapping(projection, SdkV1Symbols(model, projection, "1.6.3"))
        val output = SdkV1Generator(mapping).generate(temporary).baseDir

        assertEquals(setOf("mod.rs", "convert.rs", "compat.rs", "mapping.json"), GeneratedOutput.inventory(output).keys)
        val root = Files.readString(output.resolve("mod.rs"))
        assertTrue(root.contains("mod convert;"))
        assertTrue(root.contains("pub(crate) use convert::*;"))
        assertTrue(Regex("""#\[cfg\(feature = "sdk-v1"\)\]\s*mod compat;""").containsMatchIn(root))
        assertFalse(Regex("""#\[cfg[^\n]*\]\s*mod convert;""").containsMatchIn(root))

        val convert = Files.readString(output.resolve("convert.rs"))
        for (name in listOf(
            "checksum_algorithm_to_sdk", "checksum_algorithm_from_sdk",
            "object_to_sdk", "object_from_sdk",
            "copy_upload_input_fields_to_put_object", "copy_download_input_fields_to_get_object",
            "object_metadata_from_head_object", "update_upload_output_from_complete_multipart_upload",
        )) {
            assertTrue(convert.contains("pub(crate) fn $name("), name)
        }
        assertFalse(convert.contains("impl From<"))
        assertFalse(convert.contains("impl TryFrom<"))
        val compat = Files.readString(output.resolve("compat.rs"))
        assertTrue(compat.contains("impl From<crate::model::ChecksumAlgorithm>"))
        assertTrue(compat.contains("super::convert::checksum_algorithm_to_sdk"))
        assertTrue(compat.contains("super::convert::object_from_sdk"))
        assertFalse(compat.contains("pub(crate) fn"))
    }
}
