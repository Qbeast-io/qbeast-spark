/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import breeze.linalg.DenseMatrix
import io.qbeast.core.model.{CubeId, Point, Revision, Weight}
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import org.apache.spark.sql.Row

import scala.collection.mutable

class Vectorizer(
    revision: Revision,
    isReplication: Boolean,
    weightIndex: Int,
    bufferCapacity: Int) {

  val vectorTransformations: Seq[VectorTransformation] = revision.transformations.zipWithIndex
    .map { case (t, i) => VectorTransformation(t, i, bufferCapacity) }

  private val rowWeightsBuilder: mutable.Builder[Weight, Seq[Weight]] = Seq.newBuilder[Weight]
  rowWeightsBuilder.sizeHint(bufferCapacity)

  private val bytesBuilder: mutable.Builder[Array[Byte], Seq[Array[Byte]]] =
    Seq.newBuilder[Array[Byte]]

  bytesBuilder.sizeHint(if (isReplication) bufferCapacity else 0)
  private var rowCount: Int = 0

  def isFull: Boolean = rowCount == bufferCapacity

  def update(row: Row): Unit = {
    rowWeightsBuilder += Weight(row.getAs[Int](weightIndex))
    if (isReplication) bytesBuilder += row.getAs[Array[Byte]](cubeToReplicateColumnName)
    vectorTransformations.foreach(t => t.updateVector(rowCount, row(t.columnPosition)))
    rowCount += 1
  }

  private def getWeights: Seq[Weight] = rowWeightsBuilder.result()

  private def getCubeBytes: Seq[Option[CubeId]] = {
    if (isReplication) bytesBuilder.result().map(b => Some(revision.createCubeId(b)))
    else Seq.fill(rowCount)(None)
  }

  private def getPoints: Seq[Point] = {
    val colsAsCoords = DenseMatrix(
      vectorTransformations.map(t => t.vectorTransform(rowCount)): _*)
    (0 until rowCount).map { i =>
      val coords = colsAsCoords(::, i).toScalaVector
      Point(coords: _*)
    }
  }

  private def reset(): Unit = {
    rowWeightsBuilder.clear()
    bytesBuilder.clear()
    vectorTransformations.foreach(_.clear())
    rowCount = 0
  }

  def result(): Seq[(Point, Weight, Option[CubeId])] = {
    if (rowCount == 0) Nil
    else {
      val points = getPoints
      val rowWeights = getWeights
      val parentCubeOpts = getCubeBytes

      val results = (0 until rowCount).map { i =>
        (points(i), rowWeights(i), parentCubeOpts(i))
      }

      reset()
      results
    }
  }

}
