/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class BlockWriterTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "BlockWriter" should "write all data into files" in withSparkAndTmpDir { (spark, tmpDir) =>
    val distinctCubes = 100
    val writeTestSpec = WriteTestSpec(distinctCubes, spark, tmpDir)

    val writer = writeTestSpec.writer
    val files = writeTestSpec.indexed
      .repartition(col(cubeColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writer.writeRow)
      .collect()

    files.length shouldBe distinctCubes
  }

  it should "not miss any cubes in high partitioning" in withSparkAndTmpDir { (spark, tmpDir) =>
    val writeTestSpec = WriteTestSpec(numDistinctCubes = 400, spark, tmpDir)

    val writer = writeTestSpec.writer

    val files = writeTestSpec.indexed
      .repartition(col(cubeColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writer.writeRow)
      .collect()

    val expectedCubes = writeTestSpec.indexed
      .select(cubeColumnName)
      .collect()
      .map { row => CubeId(1, row.getAs[Array[Byte]](0)).string }
      .toSet

    val actualCubes =
      files.map(_._1).map(IndexFiles.fromAddFile(1)).flatMap(_.blocks).map(_.cubeId.string).toSet

    actualCubes.size shouldBe expectedCubes.size
    actualCubes.foreach(c => expectedCubes should (contain(c)))
  }

  it should "work with empty partitions" in withSparkAndTmpDir { (spark, tmpDir) =>
    {

      val writeTestSpec = WriteTestSpec(numDistinctCubes = 4, spark = spark, tmpDir)

      // Repartition method use spark.sql.shuffle.partitions number of partitions
      // if the number of cubes is less than the parameter
      // it will create empty partitions in the middle
      // This seems solved in 3.2.0, but we would like the code to work on that case
      val writer = writeTestSpec.writer
      writeTestSpec.indexed
        .repartition(col(cubeColumnName))
        .queryExecution
        .executedPlan
        .execute()
        .mapPartitions(part => if (Random.nextInt() % 2 == 0) Iterator.empty else part)
        .mapPartitions(writer.writeRow)
        .collect()

    }
  }

}
