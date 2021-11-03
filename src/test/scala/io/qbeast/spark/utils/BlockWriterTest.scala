/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.model.{CubeId, Point, Weight}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.utils.BlockWriterTest.IndexData
import io.qbeast.spark.index.writer.BlockWriter
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

object BlockWriterTest {

  case class IndexData(id: Long, cube: Array[Byte], weight: Int, state: String)
}

class BlockWriterTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {
  private val point = Point(0.66, 0.28)

  def loadConf(data: DataFrame): (OutputWriterFactory, SerializableConfiguration) = {
    val format = new ParquetFileFormat()
    val job = Job.getInstance()
    (
      format.prepareWrite(data.sparkSession, job, Map.empty, data.schema),
      new SerializableConfiguration(job.getConfiguration))
  }

  "BlockWriter" should "write all data into files" in withSparkAndTmpDir { (spark, tmpDir) =>
    val distinctCubes = 100
    val weightMap = 1
      .to(distinctCubes)
      .map(i => (CubeId.container(point, i), Weight(Random.nextInt())))
    val indexData =
      weightMap.map(ids => IndexData(Random.nextInt(), ids._1.bytes, ids._2.value, "FLOODED"))

    val rdd =
      spark.sparkContext.parallelize(indexData)
    val indexed =
      spark.createDataFrame(rdd).toDF("id", cubeColumnName, weightColumnName, stateColumnName)
    val data = indexed.select("id")

    val qbeastColumns = QbeastColumns(indexed)
    val (factory, serConf) = loadConf(data)
    val writer = BlockWriter(
      dataPath = tmpDir,
      schema = data.schema,
      schemaIndex = indexed.schema,
      factory = factory,
      serConf = serConf,
      qbeastColumns = qbeastColumns,
      revision = RevisionUtil.createRevisionFromDF(data, Seq("id"), 10000),
      weightMap = weightMap.toMap)

    val files = indexed
      .repartition(col(cubeColumnName), col(weightColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writer.writeRow)
      .collect()

    files.length shouldBe distinctCubes
  }

  it should "not miss any cubes in high partitioning" in withSparkAndTmpDir { (spark, tmpDir) =>
    import spark.implicits._

    val distinctCubes = 1000
    val weightMap = 1
      .to(distinctCubes)
      .map(i => (CubeId.container(point, i), Weight(Random.nextInt())))
    val indexData =
      weightMap.map(ids => IndexData(Random.nextInt(), ids._1.bytes, ids._2.value, "FLOODED"))

    val rdd =
      spark.sparkContext.parallelize(indexData)
    val indexed =
      spark.createDataFrame(rdd).toDF("id", cubeColumnName, weightColumnName, stateColumnName)
    val data = indexed.select("id")
    val names = List("id")

    val qbeastColumns = QbeastColumns(indexed)
    val (factory, serConf) = loadConf(data)
    val writer = BlockWriter(
      dataPath = tmpDir,
      schema = data.schema,
      schemaIndex = indexed.schema,
      factory = factory,
      serConf = serConf,
      qbeastColumns = qbeastColumns,
      revision = RevisionUtil.createRevisionFromDF(data, Seq("id"), 10000),
      weightMap = weightMap.toMap)

    val files = indexed
      .repartition(col(cubeColumnName), col(weightColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writer.writeRow)
      .collect()

    val dimensionCount = names.length
    val cubes = indexed
      .select(cubeColumnName)
      .map { row =>
        CubeId(dimensionCount, row.getAs[Array[Byte]](0)).string
      }
      .collect()
      .toSet
    assert(files.map(_.tags(TagUtils.cube)).forall(cube => cubes.contains(cube)))
  }
}
