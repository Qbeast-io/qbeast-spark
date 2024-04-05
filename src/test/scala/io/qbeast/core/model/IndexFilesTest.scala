package io.qbeast.core.model

import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.actions.AddFile

class IndexFilesTest extends QbeastIntegrationTestSpec {

  "IndexFiles" should "be able to create an AddFile instance from IndexFile" in withSpark { _ =>
    val indexFile = new IndexFile(
      path = "path",
      size = 2L,
      modificationTime = 0L,
      revisionId = 1L,
      blocks =
        Seq(new Block(None, CubeId.root(2), Weight(1L), Weight(2L), 1L, false)).toIndexedSeq)

    val addFile = IndexFiles.toAddFile()(indexFile)
    addFile.path shouldBe "path"
    addFile.size shouldBe 2L
    addFile.modificationTime shouldBe 0L
    addFile.getTag(TagUtils.revision) shouldBe Some("1")
    addFile.getTag(TagUtils.blocks) shouldBe Some(
      """[{"cubeId":"","minWeight":2147483647,"maxWeight":2147483647,"elementCount":1,"replicated":false}]""")

  }

  it should "be able to create an IndexFile instance from an AddFile" in withSpark { _ =>
    val addFile = AddFile(
      path = "path",
      partitionValues = Map(),
      size = 2L,
      modificationTime = 0L,
      dataChange = true,
      stats = null,
      tags = Map(
        TagUtils.revision -> "1",
        TagUtils.blocks -> """[{"cubeId":"","minWeight":2147483647,"maxWeight":2147483647,"elementCount":1,"replicated":false}]"""))

    val indexFile = IndexFiles.fromAddFile(2)(addFile)
    indexFile.path shouldBe "path"
    indexFile.size shouldBe 2L
    indexFile.modificationTime shouldBe 0L
    indexFile.revisionId shouldBe 1L
    indexFile.blocks.head.cubeId shouldBe CubeId.root(2)
    indexFile.blocks.head.elementCount shouldBe 1L
    indexFile.blocks.head.replicated shouldBe false
  }

  it should "transform the AddFile to a Remove File" in withSpark { _ =>
    val indexFile = new IndexFile(
      path = "path",
      size = 2L,
      modificationTime = 0L,
      revisionId = 1L,
      blocks =
        Seq(new Block(None, CubeId.root(2), Weight(1L), Weight(2L), 1L, false)).toIndexedSeq)

    val removeFile = IndexFiles.toRemoveFile()(indexFile)
    removeFile.path shouldBe "path"
    removeFile.dataChange shouldBe false
    removeFile.getTag(TagUtils.blocks) shouldBe None
  }

}
