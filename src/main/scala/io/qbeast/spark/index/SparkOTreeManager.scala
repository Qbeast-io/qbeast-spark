/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.model.{IndexManager, _}
import io.qbeast.spark.index.QbeastColumns.{
  cubeColumnName,
  cubeToReplicateColumnName,
  stateColumnName,
  weightColumnName
}
import io.qbeast.spark.utils.State
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager[DataFrame] with Serializable {

  /**
   * Builds an OTree index.
   * @param data the data to index
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def index(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = false)

  /**
   * Optimizes the index
   * @param data the data to optimize
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def optimize(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = true)

  /**
   * Analyzes the index
   * @param indexStatus the current status of the index
   * @return the cubes to optimize
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

  private def findTargetCubeIdsUDF(
      tableChanges: TableChanges,
      indexer: Broadcast[PointWeightIndexer]): UserDefinedFunction = {
    val revision = tableChanges.updatedRevision

    udf((rowValues: Seq[Any], weightValue: Int, parentBytes: Any) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      val parent = parentBytes match {
        case bytes: Array[Byte] => Some(revision.createCubeId(bytes))
        case _ => None
      }
      indexer.value
        .findTargetCubeIds(point, weight, parent)
        .map(_.bytes)
        .toArray
    })
  }

  private def addCubeAndState(
      weightedDataFrame: DataFrame,
      findTargetCubeIds: UserDefinedFunction,
      tableChanges: TableChanges,
      isReplication: Boolean) = {

    val revision = tableChanges.updatedRevision
    val replicatedSet = tableChanges.indexChanges.replicatedSet
    val announcedSet = tableChanges.indexChanges.announcedSet
    val columnsToIndex = revision.columnTransformers.map(_.columnName)

    weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(
          findTargetCubeIds(
            RowUtils.rowValuesColumn(columnsToIndex),
            col(weightColumnName), {
              if (isReplication) col(cubeToReplicateColumnName)
              else lit(null)
            })))
      .transform(extendWithType(columnsToIndex.length, announcedSet, replicatedSet))
      .drop(cubeToReplicateColumnName)

  }

  private def index(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    val sqlContext = SparkSession.active.sqlContext

    // Analyze the data and compute weight and estimated weight map of the result
    val (weightedDataFrame, tc) =
      DoublePassOTreeDataAnalyzer.analyze(dataFrame, indexStatus, isReplication)

    val pointWeightIndexer = PointWeightIndexer.buildNewWeightIndexer(tc.indexChanges)

    val pwiBC = sqlContext.sparkContext.broadcast(pointWeightIndexer)

    // Create UDF to find the cubeIDS
    val findTargetCubeIds = findTargetCubeIdsUDF(tc, pwiBC)

    // Add cube and state information to the dataframe
    val indexedDataFrame =
      weightedDataFrame.transform(df => addCubeAndState(df, findTargetCubeIds, tc, isReplication))

    (indexedDataFrame, tc)
  }

  private def extendWithType(
      dimensionCount: Int,
      announcedSet: Set[CubeId],
      replicatedSet: Set[CubeId]): DataFrame => DataFrame = df => {

    val states = udf { (bytes: Array[Byte]) =>
      val cubeId = CubeId(dimensionCount, bytes)
      if (announcedSet.contains(cubeId) && !replicatedSet.contains(cubeId)) {
        State.ANNOUNCED
      } else if (replicatedSet.contains(cubeId)) {
        State.REPLICATED
      } else {
        State.FLOODED
      }
    }

    df.withColumn(stateColumnName, states(col(cubeColumnName)))

  }

}
