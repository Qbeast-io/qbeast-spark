package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.spark.utils.MetadataConfig
import org.apache.hudi.common.model.HoodieWriteStat
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HudiQbeastFileUtilsTest extends AnyFlatSpec with Matchers {

  "IndexFiles" should "create IndexFile instances with correct properties from commit metadata" in {

    val dimensionCount = 2
    val hudiPath = "123e4567-e89b-12d3-a456-426614174000_0_20240101010101.parquet"
    val blocks = Seq(Block(hudiPath, CubeId.root(2), Weight(1L), Weight(2L), 1L)).toIndexedSeq

    val writeStat = new HoodieWriteStat()
    writeStat.setPath(hudiPath)
    writeStat.setFileSizeInBytes(1024)
    writeStat.setTotalWriteBytes(500)

    val commitMetadata = new HoodieCommitMetadata()
    commitMetadata.addWriteStat("partition1", writeStat)
    val extraMetadataJson =
      s"""{
         |  "$hudiPath": {
         |    "revision": 1,
         |    "blocks": [
         |      {
         |        "cubeId": "",
         |        "minWeight": 2147483647,
         |        "maxWeight": 2147483647,
         |        "elementCount": 1
         |      }
         |    ]
         |  }
         |}""".stripMargin.replaceAll("\n", "")

    commitMetadata.getExtraMetadata.put(MetadataConfig.blocks, extraMetadataJson)

    val indexFiles = HudiQbeastFileUtils.fromCommitFile(dimensionCount)(commitMetadata)

    indexFiles should have size 1
    val indexFile = indexFiles.head
    indexFile.path shouldBe hudiPath
    indexFile.size shouldBe 1024
    indexFile.revisionId shouldBe 1
    indexFile.blocks shouldBe blocks
  }

  it should "correctly transform IndexFile into WriteStatus" in {
    val hudiPath = "123e4567-e89b-12d3-a456-426614174000_0_20240101010101.parquet"
    val indexFile = IndexFile(
      path = hudiPath,
      size = 1024,
      dataChange = true,
      modificationTime = 123456789L,
      revisionId = 1,
      blocks = Seq(Block(hudiPath, CubeId.root(2), Weight(1L), Weight(2L), 1L)).toIndexedSeq)

    val totalWriteTime = 500L
    val writeStatus = HudiQbeastFileUtils.toWriteStat(indexFile, totalWriteTime)

    writeStatus.getStat.getPath shouldBe hudiPath
    writeStatus.getTotalRecords shouldBe 1
    writeStatus.getStat.getFileSizeInBytes shouldBe 1024
    writeStatus.getStat.getRuntimeStats.getTotalCreateTime shouldBe totalWriteTime
  }

  it should "serialize blocks into correct JSON format" in {
    val blocks = Seq(
      Block("file1", CubeId.root(2), Weight(1L), Weight(2L), 1L),
      Block("file2", CubeId.root(2), Weight(2L), Weight(3L), 2L)).toIndexedSeq

    val json = HudiQbeastFileUtils.encodeBlocks(blocks)

    json shouldBe
      """[
        |{"cubeId":"","minWeight":2147483647,"maxWeight":2147483647,"elementCount":1},
        |{"cubeId":"","minWeight":2147483647,"maxWeight":2147483647,"elementCount":2}
        |]""".stripMargin.replaceAll("\n", "")
  }

}
