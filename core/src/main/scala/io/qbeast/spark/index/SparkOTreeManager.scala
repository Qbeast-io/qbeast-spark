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
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.addRandomWeight
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager with Serializable with Logging {

  /**
   * Builds an OTree index.
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the changes to the index
   */
  override def index(
      data: DataFrame,
      indexStatus: IndexStatus,
      options: QbeastOptions): (DataFrame, TableChanges) = {
    // If the DataFrame is empty, we return an empty table changes
    if (data.isEmpty) {
      logInfo("Indexing empty Dataframe. Returning empty table changes.")
      val emptyTableChanges =
        BroadcastTableChanges(
          None,
          indexStatus,
          Map.empty[CubeId, Weight],
          Map.empty[CubeId, Long])
      return (data, emptyTableChanges)
    }
    // Analyze the data, add weight column, compute cube domains, and compute cube weights
    val (weightedDataFrame, tc) = DoublePassOTreeDataAnalyzer.analyze(data, indexStatus, options)

    // Add cube column
    val pointWeightIndexer = new SparkPointWeightIndexer(tc)
    val indexedDataFrame = weightedDataFrame.transform(pointWeightIndexer.buildIndex)
    (indexedDataFrame, tc)
  }

  /**
   * Optimizes the input data by reassigning cubes according to the current index status
   *
   * @param data
   *   the data to optimize
   * @param indexStatus
   *   the current index status
   * @return
   *   the optimized data and the changes of the index
   */
  override def optimize(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    val spark = data.sparkSession
    val revision = indexStatus.revision
    logTrace(s"""Begin: Analyze Optimize for index with
                |revision=$revision""".stripMargin.replaceAll("\n", " "))

    // Add a random weight column
    val weightedDataFrame = data.transform(addRandomWeight(revision))
    val tcForIndexing = BroadcastTableChanges(
      isNewRevision = false,
      revision,
      spark.sparkContext.broadcast(indexStatus.cubeMaxWeights()),
      spark.sparkContext.broadcast(Map.empty[CubeId, Long]))

    // Add cube column
    val pointWeightIndexer = new SparkPointWeightIndexer(tcForIndexing)
    val indexedDataFrame = weightedDataFrame.transform(pointWeightIndexer.buildIndex)

    import spark.implicits._
    val optimizedDataBlockSizes: Map[CubeId, Long] = indexedDataFrame
      .groupBy(cubeColumnName)
      .count()
      .as[(Array[Byte], Long)]
      .map(row => revision.createCubeId(row._1) -> row._2)
      .collect()
      .toMap

    (
      indexedDataFrame,
      tcForIndexing.copy(inputBlockElementCountsBroadcast =
        spark.sparkContext.broadcast(optimizedDataBlockSizes)))
  }

}
