/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, PointWeightIndexer, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, stateColumnName, weightColumnName}
import io.qbeast.spark.utils.State
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

private class SparkPointWeightIndexer(tableChanges: TableChanges, isReplication: Boolean)
    extends Serializable {

  val findTargetCubeIdsUDF: UserDefinedFunction = {
    val revision = tableChanges.updatedRevision
    val pointIndexer: PointWeightIndexer = PointWeightIndexer(tableChanges)

    udf((rowValues: Row, weightValue: Int) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      pointIndexer
        .findTargetCubeIds(point, weight)
        .map(_.bytes)
        .toArray
    })
  }

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

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {

    val revision = tableChanges.updatedRevision
    val columnsToIndex = revision.columnTransformers.map(_.columnName)

    // TODO: Add replication stuff
    weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(findTargetCubeIdsUDF(struct(columnsToIndex.map(col): _*), col(weightColumnName))))
      .transform(
        addState(
          dimensionCount = columnsToIndex.length,
          replicatedSet = tableChanges.replicatedSet,
          announcedSet = tableChanges.announcedSet))

  }

}
