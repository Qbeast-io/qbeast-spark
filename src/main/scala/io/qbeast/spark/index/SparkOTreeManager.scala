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
import io.qbeast.IISeq
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager[DataFrame] with Serializable with Logging {

  /**
   * Builds an OTree index.
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the changes to the index
   */
  override def index(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = false)

  /**
   * Optimizes the index
   * @param data
   *   the data to optimize
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the changes to the index
   */
  override def optimize(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = true)

  /**
   * Analyzes the index
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the cubes to optimize
   */
  override def analyze(indexStatus: IndexStatus): IISeq[CubeId] = {
    findCubesToOptimize(indexStatus)
  }

  // PRIVATE METHODS //

  private def findCubesToOptimize(indexStatus: IndexStatus): IISeq[CubeId] = {
    val overflowedSet = indexStatus.overflowedSet
    val replicatedSet = indexStatus.replicatedSet

    val cubesToOptimize = overflowedSet
      .filter(cube => {
        !replicatedSet.contains(cube) && (cube.parent match {
          case None => true
          case Some(p) => replicatedSet.contains(p)
        })
      })

    if (cubesToOptimize.isEmpty && replicatedSet.isEmpty) {
      Seq(indexStatus.revision.createCubeIdRoot()).toIndexedSeq
    } else cubesToOptimize.toIndexedSeq
  }

  private def index(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    logTrace(s"Begin: Index with revision ${indexStatus.revision}")
    // Analyze the data and compute weight and estimated weight map of the result
    val (weightedDataFrame, tc) =
      DoublePassOTreeDataAnalyzer.analyzeAppend(dataFrame, indexStatus, isReplication)

    val pointWeightIndexer = new SparkPointWeightIndexer(tc, isReplication)

    // Add cube and state information to the dataframe
    val indexedDataFrame =
      weightedDataFrame.transform(pointWeightIndexer.buildIndex)

    val result = (indexedDataFrame, tc)
    logTrace(s"End: Index with revision ${indexStatus.revision}")
    result
  }

}
