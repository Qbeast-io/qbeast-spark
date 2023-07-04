package io.qbeast.spark.delta.writer

import io.qbeast.TestClasses.IndexData
import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.index.{NormalizedWeight, QbeastColumns, SparkRevisionFactory}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID
import scala.collection.immutable
import scala.collection.immutable.SortedMap
import scala.util.Random

case class WriteTestSpec(numDistinctCubes: Int, spark: SparkSession, tmpDir: String) {

  def loadConf(data: DataFrame): (OutputWriterFactory, SerializableConfiguration) = {
    val format = new ParquetFileFormat()
    val job = Job.getInstance()
    (
      format.prepareWrite(data.sparkSession, job, Map.empty, data.schema),
      new SerializableConfiguration(job.getConfiguration))
  }

  val tableID: QTableID = QTableID(tmpDir)

  val point: Point = Point(0.66, 0.28)

  val weightMap: Map[CubeId, NormalizedWeight] = 1
    .to(numDistinctCubes)
    .map(i => (CubeId.container(point, i), Random.nextDouble()))
    .toMap

  val announcedSet: Set[CubeId] = Set.empty
  val replicatedSet: Set[CubeId] = Set.empty

  val indexData: immutable.IndexedSeq[IndexData] =
    weightMap.toIndexedSeq.map(ids =>
      IndexData(Random.nextInt(), ids._1.bytes, ids._2, "FLOODED"))

  val rdd: RDD[IndexData] =
    spark.sparkContext.parallelize(indexData)

  val indexed: DataFrame =
    spark.createDataFrame(rdd).toDF("id", cubeColumnName, weightColumnName, stateColumnName)

  val data: DataFrame = indexed.select("id")

  val indexedColumns: Seq[String] = Seq("id")

  val qbeastColumns: QbeastColumns = QbeastColumns(indexed)

  val (factory, serConf) = loadConf(data)

  val rev: Revision = SparkRevisionFactory
    .createNewRevision(
      QTableID("test"),
      data.schema,
      Map("columnsToIndex" -> "id", "cubeSize" -> "10000"))

  val cubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val cubeStatusesSeq = weightMap.toIndexedSeq.map {
      case (cubeId: CubeId, normalizedWeight: NormalizedWeight) =>
        val maxWeight = Weight(normalizedWeight)
        val files = 1
          .to(4)
          .map(i => {
            // Create a QbeastBlock under the revision
            QbeastBlock(
              UUID.randomUUID().toString,
              cubeId.string,
              rev.revisionID,
              Weight.MinValue,
              maxWeight,
              false,
              i * 10,
              i * 1000L,
              System.currentTimeMillis())
          })
        (cubeId, CubeStatus(cubeId, maxWeight, normalizedWeight, files))
    }

    SortedMap(cubeStatusesSeq: _*)
  }

  val indexStatus: IndexStatus = IndexStatus(rev, Set.empty, Set.empty, cubeStatuses)

  val tableChanges: TableChanges =
    BroadcastedTableChanges(None, IndexStatus(rev), deltaNormalizedCubeWeights = weightMap)

  val writer: BlockWriter = new BlockWriter(
    dataPath = tmpDir,
    schema = data.schema,
    schemaIndex = indexed.schema,
    factory = factory,
    serConf = serConf,
    statsTrackers = Seq.empty,
    qbeastColumns = qbeastColumns,
    tableChanges = tableChanges)

  def writeData(): Unit = {

    cubeStatuses.foreach { case (cubeId: CubeId, cubeStatus: CubeStatus) =>
      cubeStatus.files.foreach { i =>
        // Write data in parquetFile
        val dataCube = indexed.where(s"$cubeColumnName == '${cubeId.string}'")
        dataCube.coalesce(1).write.format("parquet").save(tmpDir + "/" + i.path)
      }

    }
  }

}
