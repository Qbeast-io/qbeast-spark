/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{PointWeightIndexer, TableChanges, Weight}
import io.qbeast.core.transform.LearnedStringTransformer
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.index.transformations.Vectorizer
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

private class SparkPointWeightIndexer(tableChanges: TableChanges, isReplication: Boolean)
    extends Serializable {

  private val revision = tableChanges.updatedRevision
  private val pointIndexer: PointWeightIndexer = PointWeightIndexer(tableChanges)

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
    val vectorize = revision.columnTransformers.exists {
      case _: LearnedStringTransformer => true
      case _ => false
    }

    if (vectorize) {
      val weightIndex = weightedDataFrame.schema.fieldIndex(weightColumnName)
      val columnIndices =
        revision.columnTransformers.map(t => weightedDataFrame.schema.fieldIndex(t.columnName))
      val bufferCapacity = CUBE_WEIGHTS_BUFFER_CAPACITY
      val schema = weightedDataFrame.schema.add(cubeColumnName, BinaryType, nullable = true)
      val rdd = weightedDataFrame.rdd.mapPartitions { rows =>
        val vectorizer =
          new Vectorizer(
            revision,
            isReplication,
            weightIndex,
            columnIndices,
            bufferCapacity.toInt)
        vectorizer.vectorizedIndexing(rows, pointIndexer)
      }

      val spark = SparkSession.active
      spark.createDataFrame(rdd, schema)
    } else {
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
      } else {
        weightedDataFrame
          .withColumn(
            cubeColumnName,
            explode(
              findTargetCubeIdsUDF(struct(columnsToIndex.map(col): _*), col(weightColumnName))))
      }
    }
  }

}
