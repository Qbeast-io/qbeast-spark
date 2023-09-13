/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import breeze.linalg.DenseVector
import io.qbeast.core.transform.{
  HashTransformation,
  IdentityToZeroTransformation,
  LearnedStringTransformation,
  LinearTransformation,
  NullToZeroTransformation,
  Transformation
}

trait VectorTransformation {
  val transformation: Transformation
  val columnPosition: Int
  val columnSize: Int

  def updateVector(i: Int, value: Any): Unit

  def vectorTransform(length: Int): DenseVector[Double]

  def clear(): Unit
}

object VectorTransformation {

  def apply(
      transformation: Transformation,
      columnPosition: Int,
      columnSize: Int): VectorTransformation = transformation match {
    case t: LinearTransformation =>
      new VectorizedLinearTransformation(t, columnPosition, columnSize)
    case t: LearnedStringTransformation =>
      new VectorizedLearnedStringTransformation(t, columnPosition, columnSize)
    case t: HashTransformation =>
      new VectorizedHashTransformation(t, columnPosition, columnSize)
    case t: IdentityToZeroTransformation =>
      new VectorizedIdentityToZeroTransformation(t, columnPosition, columnSize)
    case NullToZeroTransformation =>
      new VectorizedIdentityToZeroTransformation(
        NullToZeroTransformation,
        columnPosition,
        columnSize)
  }

}
