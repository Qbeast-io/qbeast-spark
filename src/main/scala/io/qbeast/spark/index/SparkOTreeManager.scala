/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import org.apache.spark.sql.DataFrame

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager[DataFrame] with Serializable {

  /**
   * Indexes given data.
   *
   * @param data the data to index
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def index(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = false)

  /**
   * Replicates given index data.
   *
   * @param data the data to optimize
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def replicate(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = true)

  /**
   * Analyzes the index and return the cubes which need replication.
   *
   * @param indexStatus the current status of the index
   * @return the cubes to replicate
   */
  override def analyze(indexStatus: IndexStatus): IISeq[CubeId] = {
    findCubesToReplicate(indexStatus)
  }

  // PRIVATE METHODS //

  private def findCubesToReplicate(indexStatus: IndexStatus): IISeq[CubeId] = {
    val overflowedSet = indexStatus.overflowedSet
    val replicatedSet = indexStatus.replicatedSet

    val cubesToReplicate = overflowedSet
      .filter(cube => {
        !replicatedSet.contains(cube) && (cube.parent match {
          case None => true
          case Some(p) => replicatedSet.contains(p)
        })
      })

    if (cubesToReplicate.isEmpty && replicatedSet.isEmpty) {
      Seq(indexStatus.revision.createCubeIdRoot()).toIndexedSeq
    } else cubesToReplicate.toIndexedSeq
  }

  private def index(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    // Analyze the data and compute weight and estimated weight map of the result
    val (weightedDataFrame, tc) =
      DoublePassOTreeDataAnalyzer.analyze(dataFrame, indexStatus, isReplication)

    val pointWeightIndexer = new SparkPointWeightIndexer(tc, isReplication)

    // Add cube and state information to the dataframe
    val indexedDataFrame =
      weightedDataFrame.transform(pointWeightIndexer.buildIndex)

    (indexedDataFrame, tc)
  }

}
