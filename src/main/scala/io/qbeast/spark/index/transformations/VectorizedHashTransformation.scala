/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import breeze.linalg.DenseVector
import io.qbeast.core.transform.HashTransformation

import scala.collection.mutable

class VectorizedHashTransformation(
    override val transformation: HashTransformation,
    override val columnPosition: Int,
    override val columnSize: Int)
    extends VectorTransformation {

  private val vectorBuilder: mutable.Builder[Any, Vector[Any]] = Vector.newBuilder[Any]

  override def updateVector(i: Int, value: Any): Unit = vectorBuilder += value

  override def vectorTransform(length: Int): DenseVector[Double] = {
    var vector = vectorBuilder.result()
    if (length < vector.length) vector = vector.take(length)
    DenseVector(transformation.transformColumn(vector): _*)
  }

  override def clear(): Unit = vectorBuilder.clear()
}
