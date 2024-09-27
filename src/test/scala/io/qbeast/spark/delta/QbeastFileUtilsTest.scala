/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import com.fasterxml.jackson.core.JsonParseException
import io.qbeast.core.model._
import io.qbeast.spark.utils.TagUtils
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.RemoveFile

class QbeastFileUtilsTest extends QbeastIntegrationTestSpec {

  "IndexFiles" should "be able to create an AddFile instance from IndexFile" in withSpark { _ =>
    val indexFile = IndexFile(
      path = "path",
      size = 2L,
      modificationTime = 0L,
      revisionId = 1L,
      blocks = Seq(
        Block(
          "path",
          CubeId.root(2),
          Weight(1L),
          Weight(2L),
          1L,
          replicated = false)).toIndexedSeq)

    val addFile = DeltaQbeastFileUtils.toAddFile(dataChange = true)(indexFile)
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

    val indexFile = DeltaQbeastFileUtils.fromAddFile(2)(addFile)
    indexFile.path shouldBe "path"
    indexFile.size shouldBe 2L
    indexFile.modificationTime shouldBe 0L
    indexFile.revisionId shouldBe 1L
    indexFile.blocks.head.cubeId shouldBe CubeId.root(2)
    indexFile.blocks.head.elementCount shouldBe 1L
    indexFile.blocks.head.replicated shouldBe false
  }

  it should "transform the DeleteFile to a RemoveFile" in withSpark { _ =>
    val deleteFile = DeleteFile(path = "path", size = 2L, deletionTimestamp = 0L)

    val dataChange = false
    val removeFile = DeltaQbeastFileUtils.toRemoveFile(dataChange = dataChange)(deleteFile)
    removeFile.path shouldBe "path"
    removeFile.dataChange shouldBe dataChange
  }

  it should "transform the RemoveFile to a DeleteFile" in withSpark { _ =>
    val removeFile = RemoveFile(
      path = "path",
      partitionValues = Map(),
      size = Some(2L),
      deletionTimestamp = Some(0L),
      dataChange = true,
      stats = null)

    val deleteFile = DeltaQbeastFileUtils.fromRemoveFile(removeFile)
    deleteFile.path shouldBe "path"
    deleteFile.size shouldBe 2L
    deleteFile.deletionTimestamp shouldBe 0L
  }

  it should "be able to create a FileStatus from an IndexFile" in withSpark { _ =>
    val indexFile = IndexFile(
      path = "path",
      size = 2L,
      modificationTime = 0L,
      revisionId = 1L,
      blocks = Seq(
        Block(
          "path",
          CubeId.root(2),
          Weight(1L),
          Weight(2L),
          1L,
          replicated = false)).toIndexedSeq)

    val indexPath = new Path("/absolute/")
    val indexStatus = DeltaQbeastFileUtils.toFileStatus(indexPath)(indexFile)

    indexStatus.isFile shouldBe true
    indexStatus.getPath shouldBe new Path("/absolute/path")
    indexStatus.getLen shouldBe 2L
    indexStatus.getModificationTime shouldBe 0L

  }

  it should "be able to create a FileStatusWithMetadata from IndexFile" in withSpark { _ =>
    val indexFile = IndexFile(
      path = "path",
      size = 2L,
      modificationTime = 0L,
      revisionId = 1L,
      blocks = Seq(
        Block(
          "path",
          CubeId.root(2),
          Weight(1L),
          Weight(2L),
          1L,
          replicated = false)).toIndexedSeq)

    val indexPath = new Path("/absolute/")
    val indexStatus =
      DeltaQbeastFileUtils.toFileStatusWithMetadata(indexPath, Map("key" -> "value"))(indexFile)

    indexStatus.getPath shouldBe new Path("/absolute/path")
    indexStatus.getLen shouldBe 2L
    indexStatus.getModificationTime shouldBe 0L
    indexStatus.metadata shouldBe Map("key" -> "value")

  }

  it should "throw error when trying to create an IndexFile with a wrong block format start" in withSpark {
    _ =>
      val addFile = AddFile(
        path = "path",
        partitionValues = Map(),
        size = 2L,
        modificationTime = 0L,
        dataChange = true,
        stats = null,
        tags = Map(
          TagUtils.revision -> "1",
          TagUtils.blocks -> // WRONG BLOCK START
            """{"cubeId":"","minWeight":2147483647,
              |"maxWeight":2147483647,"elementCount":1,"replicated":false}]""".stripMargin))

      an[JsonParseException] shouldBe thrownBy(DeltaQbeastFileUtils.fromAddFile(2)(addFile))
  }

  it should "throw error when trying to create an IndexFile with a wrong block format end" in withSpark {
    _ =>
      val addFile = AddFile(
        path = "path",
        partitionValues = Map(),
        size = 2L,
        modificationTime = 0L,
        dataChange = true,
        stats = null,
        tags = Map(
          TagUtils.revision -> "wrong_revision",
          TagUtils.blocks ->
            """[{"cubeId":"","minWeight":2147483647,
              |"maxWeight":2147483647,"elementCount":1,"replicated":false}""".stripMargin))

      an[NumberFormatException] shouldBe thrownBy(DeltaQbeastFileUtils.fromAddFile(2)(addFile))
  }

}
