/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{PointWeightIndexer, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, weightColumnName}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

private object SparkPointWeightIndexer {}

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

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {

    val revision = tableChanges.updatedRevision
    val columnsToIndex = revision.columnTransformers.map(_.columnName)

    weightedDataFrame
      .withColumn(
        cubeColumnName,
        explode(findTargetCubeIdsUDF(struct(columnsToIndex.map(col): _*), col(weightColumnName))))

  }

}
