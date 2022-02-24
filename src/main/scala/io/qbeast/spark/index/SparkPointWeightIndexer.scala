/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model.{CubeId, PointWeightIndexer, TableChanges, Weight}
import io.qbeast.core.transform.Transformation
import io.qbeast.spark.index.QbeastColumns.{
  cubeColumnName,
  cubeToReplicateColumnName,
  stateColumnName,
  weightColumnName
}
import io.qbeast.spark.index.SparkPointWeightIndexer.addState
import io.qbeast.spark.utils.State
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

private object SparkPointWeightIndexer {

  def addState(
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

private class SparkPointWeightIndexer(
    tableChanges: TableChanges,
    isReplication: Boolean,
    transformations: IISeq[Transformation] = Seq().toIndexedSeq)
    extends Serializable {

  val pointIndexer: PointWeightIndexer =
    PointWeightIndexer(tableChanges)

  val pwiBC: Broadcast[PointWeightIndexer] =
    SparkSession.active.sparkContext.broadcast(pointIndexer)

  val findTargetCubeIdsUDF: UserDefinedFunction = {
    var revision = tableChanges.updatedRevision
    if (transformations.nonEmpty) {
      revision = revision.copy(transformations = transformations)
    }

    udf((rowValues: Row, weightValue: Int, parentBytes: Any) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      val parent = parentBytes match {
        case bytes: Array[Byte] => Some(revision.createCubeId(bytes))
        case _ => None
      }
      pwiBC.value
        .findTargetCubeIds(point, weight, parent)
        .map(_.bytes)
        .toArray
    })
  }

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {

    val revision = tableChanges.updatedRevision
    val replicatedSet = tableChanges.replicatedSet
    val announcedSet = tableChanges.announcedSet
    val columnsToIndex = revision.columnTransformers.map(_.columnName)

    weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(
          findTargetCubeIdsUDF(
            struct(columnsToIndex.map(col): _*),
            col(weightColumnName), {
              if (isReplication) col(cubeToReplicateColumnName)
              else lit(null)
            })))
      .transform(addState(columnsToIndex.length, announcedSet, replicatedSet))
      .drop(cubeToReplicateColumnName)

  }

}
