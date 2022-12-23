/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, PointWeightIndexer, TableChanges, Weight}
import io.qbeast.spark.index.QbeastColumns._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

private class SparkPointWeightIndexer(tableChanges: TableChanges, isReplication: Boolean)
    extends Serializable {

  private val revision = tableChanges.updatedRevision
  private val pointIndexer: PointWeightIndexer = PointWeightIndexer(tableChanges)

  private def compressionMapping(cube: CubeId): CompressionResult = {
    if (tableChanges.announcedOrReplicatedSet.contains(cube)) {
      CompressionResult(cube.bytes, isCompressed = false)
    } else {
      val mappedCube = tableChanges.compressionMap.getOrElse(cube, cube)
      CompressionResult(mappedCube.bytes, mappedCube != cube)
    }
  }

  private val findTargetCubeIdsNewRevisionUDF: UserDefinedFunction = {

    udf((rowValues: Row, weightValue: Int) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      pointIndexer
        .findTargetCubeIds(point, weight)
        .map(compressionMapping)
        .toArray
    })
  }

  private val findTargetCubeIdsUDF: UserDefinedFunction = {

    udf((rowValues: Row, weightValue: Int) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      pointIndexer
        .findTargetCubeIds(point, weight)
        .map(_.bytes)
        .toArray
    })
  }

  private val replicateTargetCubeIds: UserDefinedFunction = {

    udf((rowValues: Row, weightValue: Int, parentBytes: Array[Byte]) => {
      val point = RowUtils.rowValuesToPoint(rowValues, revision)
      val weight = Weight(weightValue)
      val parent = revision.createCubeId(parentBytes)
      pointIndexer
        .findTargetCubeIds(point, weight, Some(parent))
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
            replicateTargetCubeIds(
              struct(columnsToIndex.map(col): _*),
              col(weightColumnName),
              col(cubeToReplicateColumnName))))
    } else if (tableChanges.isNewRevision) {
      weightedDataFrame
        .withColumn(
          cubeAndIsCompressionColumnName,
          explode(
            findTargetCubeIdsNewRevisionUDF(
              struct(columnsToIndex.map(col): _*),
              col(weightColumnName))))
        .select(
          weightedDataFrame.columns.map(col) :+
            col(s"$cubeAndIsCompressionColumnName.cubeBytes").as(cubeColumnName) :+
            col(s"$cubeAndIsCompressionColumnName.isCompressed").as(isCompressedColumnName): _*)
    } else {
      weightedDataFrame
        .withColumn(
          cubeColumnName,
          explode(
            findTargetCubeIdsUDF(struct(columnsToIndex.map(col): _*), col(weightColumnName))))
    }
  }

}

case class CompressionResult(cubeBytes: Array[Byte], isCompressed: Boolean)
