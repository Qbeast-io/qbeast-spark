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
package io.qbeast.spark.writer

import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.TestClasses.IndexData
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
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

  val weightMap: Map[CubeId, Weight] = 1
    .to(numDistinctCubes)
    .map(i => (CubeId.container(point, i), NormalizedWeight.toWeight(Random.nextDouble())))
    .toMap

  val indexData: immutable.IndexedSeq[IndexData] =
    weightMap.toIndexedSeq.map(ids => IndexData(Random.nextInt(), ids._1.bytes, ids._2.fraction))

  import spark.implicits._
  val indexed: DataFrame = indexData.toDF("id", cubeColumnName, weightColumnName)

  val data: DataFrame = indexed.select("id")

  val indexedColumns: Seq[String] = Seq("id")

  val qbeastColumns: QbeastColumns = QbeastColumns(indexed)

  val (factory, serConf) = loadConf(data)

  val rev: Revision = SparkRevisionFactory
    .createNewRevision(
      QTableID("test"),
      data.schema,
      QbeastOptions(Map("columnsToIndex" -> "id", "cubeSize" -> "10000")))

  val cubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val cubeStatusesSeq = weightMap.toIndexedSeq.map { case (cubeId: CubeId, maxWeight: Weight) =>
      val blocks = (1 to 4)
        .map { i =>
          // Create a Block under the revision
          new IndexFileBuilder()
            .setPath(UUID.randomUUID().toString)
            .setSize(i * 1000L)
            .setModificationTime(System.currentTimeMillis())
            .setRevisionId(rev.revisionID)
            .beginBlock()
            .setCubeId(cubeId)
            .setMaxWeight(maxWeight)
            .setElementCount(i * 10L)
            .endBlock()
            .result()
        }
        .flatMap(_.blocks)
      (cubeId, CubeStatus(cubeId, maxWeight, maxWeight.fraction, blocks))
    }

    SortedMap(cubeStatusesSeq: _*)
  }

  val indexStatus: IndexStatus = IndexStatus(rev, cubeStatuses)

  val tableChanges: TableChanges =
    BroadcastTableChanges(
      None,
      IndexStatus(rev),
      updatedCubeNormalizedWeights = weightMap,
      Map.empty,
      isOptimizationOperation = false)

}
