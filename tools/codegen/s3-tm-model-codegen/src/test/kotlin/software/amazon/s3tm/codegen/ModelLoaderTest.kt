/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.smithy.model.shapes.ServiceShape

class ModelLoaderTest {
    @TempDir
    lateinit var temporary: Path

    @Test
    fun `loads a local S3 fixture without fetching`() {
        val resource = javaClass.getResource("/s3-minimal.json")!!
        val model = ModelLoader.load(Path.of(resource.toURI()))
        assertEquals(
            1,
            model.expectShape(ModelLoader.serviceId, ServiceShape::class.java).operations.size,
        )
    }

    @Test
    fun `rejects unresolved references through Smithy validation`() {
        val input = temporary.resolve("invalid.json")
        Files.writeString(
            input,
            """{"smithy":"2.0","shapes":{"com.amazonaws.s3#AmazonS3":{"type":"service","version":"2006-03-01","operations":[{"target":"com.amazonaws.s3#Missing"}]}}}""",
        )
        assertThrows(RuntimeException::class.java) { ModelLoader.load(input) }
    }

    @Test
    fun `requires the S3 service`() {
        val input = temporary.resolve("other.json")
        Files.writeString(input, """{"smithy":"2.0","shapes":{}}""")
        assertThrows(RuntimeException::class.java) { ModelLoader.load(input) }
    }
}
