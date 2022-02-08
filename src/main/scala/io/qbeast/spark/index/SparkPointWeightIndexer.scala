/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{PointWeightIndexer, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

private class SparkPointWeightIndexer(tableChanges: TableChanges, isReplication: Boolean)
    extends Serializable {

  private val findTargetCubeIdsUDF: UserDefinedFunction = {

    val revision = tableChanges.updatedRevision
    val pointIndexer: PointWeightIndexer = PointWeightIndexer(tableChanges)

    udf((rowValues: Row, weightValue: Int, parentBytes: Any) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      val parent = parentBytes match {
        case bytes: Array[Byte] => Some(revision.createCubeId(bytes))
        case _ => None
      }
      pointIndexer
        .findTargetCubeIds(point, weight, parent)
        .map(_.bytes)
        .toArray
    })
  }

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {

    val revision = tableChanges.updatedRevision
    val columnsToIndex = revision.columnTransformers.map(_.columnName)
    if (isReplication) {
      weightedDataFrame
        .withColumn(
          cubeColumnName,
          explode(
            findTargetCubeIdsUDF(
              struct(columnsToIndex.map(col): _*),
              col(weightColumnName),
              col(cubeToReplicateColumnName))))
    } else {
      weightedDataFrame
        .withColumn(
          cubeColumnName,
          explode(
            findTargetCubeIdsUDF(
              struct(columnsToIndex.map(col): _*),
              col(weightColumnName),
              lit(null))))
    }

  }

}
