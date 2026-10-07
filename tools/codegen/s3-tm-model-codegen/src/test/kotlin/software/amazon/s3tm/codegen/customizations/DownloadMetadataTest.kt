/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.s3tm.codegen.customizations

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.s3tm.codegen.ModelGenerator
import software.amazon.s3tm.codegen.ModelLoader
import software.amazon.s3tm.codegen.TmModelProjection
import software.amazon.smithy.model.shapes.StringShape
import software.amazon.smithy.model.shapes.StructureShape
import software.amazon.smithy.model.traits.DocumentationTrait

class DownloadMetadataTest {
    @TempDir
    lateinit var temporary: Path

    private fun source() = ModelLoader.load(Path.of(javaClass.getResource("/s3-example-model.smithy")!!.toURI()))

    @Test
    fun `metadata defaults build empty optional service values`() {
        val generated = ModelGenerator.generate(TmModelProjection.project(source()), temporary, "1.6.3")
        for ((name, file) in listOf("ObjectMetadata" to "_object_metadata.rs", "ChunkMetadata" to "_chunk_metadata.rs")) {
            val rust = Files.readString(generated.baseDir.resolve("src/model/$file"))
            assertTrue(rust.contains("impl ::std::default::Default for $name"))
            assertTrue(rust.contains("Self::builder().build()"))
        }
    }

    @Test
    fun `member docs override target descriptions and discovery path availability is specific`() {
        val original = source()
        val described = StringShape.builder().id(TmModelProjection.id("DescribedMetadata"))
            .addTrait(DocumentationTrait("Inherited service meaning.")).build()
        val get = original.expectShape(TmModelProjection.id("GetObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("GetOnly", described.id).build()
        val head = original.expectShape(TmModelProjection.id("HeadObjectOutput"), StructureShape::class.java)
            .toBuilder().addMember("HeadOnly", described.id)
            .addMember("MemberOverride", described.id) {
                it.addTrait(DocumentationTrait("Member-specific meaning."))
            }.build()
        val projection = TmModelProjection.project(original.toBuilder().addShapes(described, get, head).build())
        val objectShape = projection.model.expectShape(TmModelProjection.tmId("ObjectMetadata"), StructureShape::class.java)
        val chunkShape = projection.model.expectShape(TmModelProjection.tmId("ChunkMetadata"), StructureShape::class.java)
        fun docs(shape: StructureShape, name: String) =
            shape.getMember(name).orElseThrow().expectTrait(DocumentationTrait::class.java).value
        assertTrue(docs(objectShape, "GetOnly").contains("Inherited service meaning."))
        assertTrue(docs(objectShape, "GetOnly").contains("discovery uses a GET"))
        assertTrue(docs(objectShape, "HeadOnly").contains("discovery uses a HEAD"))
        assertTrue(docs(objectShape, "MemberOverride").startsWith("Member-specific meaning."))
        assertEquals("Inherited service meaning.", docs(chunkShape, "GetOnly"))
        val generated = ModelGenerator.generate(projection, temporary, "1.6.3")
        val rust = Files.readString(generated.baseDir.resolve("src/model/_object_metadata.rs"))
        assertTrue(rust.contains("Inherited service meaning."))
        assertTrue(rust.contains("discovery uses a HEAD"))
        assertTrue(rust.contains("do not by themselves"))
    }
}
