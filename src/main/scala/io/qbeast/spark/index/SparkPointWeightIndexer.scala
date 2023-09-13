/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{Point, PointWeightIndexer, TableChanges, Weight}
import io.qbeast.core.transform.LearnedStringTransformer
import io.qbeast.spark.index.QbeastColumns._
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

  def vectorizedIndexing(
      rows: Iterator[Row],
      weightIndex: Int,
      targetIndices: Seq[Int]): Iterator[Row] = {
    val (rowsForPoints, originalRows) = rows.duplicate
    val rowWeightsBuilder = Seq.newBuilder[Weight]
    val bytesBuilder = Seq.newBuilder[Array[Byte]]

    val coords = rowsForPoints.toSeq
      .map { row =>
        rowWeightsBuilder += Weight(row.getAs[Int](weightIndex))
        if (isReplication) bytesBuilder += row.getAs[Array[Byte]](cubeToReplicateColumnName)
        targetIndices.map(i => row(i))
      }
      .transpose
      .zip(revision.transformations)
      .map { case (column, t) => t.transformColumn(column) }
      .transpose

    val rowWeights = rowWeightsBuilder.result()
    val bytes = bytesBuilder.result()

    originalRows.zipWithIndex.flatMap { case (row, i) =>
      val (point, weight) = (Point(coords(i): _*), rowWeights(i))
      val parent = if (isReplication) Some(revision.createCubeId(bytes(i))) else None
      pointIndexer
        .findTargetCubeIds(point, weight, parent)
        .map(c => Row(row.toSeq :+ c.bytes: _*))
    }
  }

  def buildIndex: DataFrame => DataFrame = (weightedDataFrame: DataFrame) => {
    val revision = tableChanges.updatedRevision
    val vectorize = revision.columnTransformers.exists {
      case _: LearnedStringTransformer => true
      case _ => false
    }

    if (vectorize) {
      val spark = SparkSession.active
      val weightIndex = weightedDataFrame.schema.fieldIndex(weightColumnName)
      val columnIndices =
        revision.columnTransformers.map(t => weightedDataFrame.schema.fieldIndex(t.columnName))
      val schema = weightedDataFrame.schema.add(cubeColumnName, BinaryType, nullable = true)
      val rdd =
        weightedDataFrame.rdd.mapPartitions(vectorizedIndexing(_, weightIndex, columnIndices))
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
