/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.transform

import io.qbeast.mapper

case class TransformationSerialization(name: String, value: Transformation)

object TransformationSerializer {

  def loadFromJson(value: String): Transformation = {

    mapper.readValue[TransformationSerialization](value).value

  }

}

/**
 * Double value transformation.
 */
trait Transformation {

  /**
   * Converts a real number to a normalized value.
   *
   * @param value a real number to convert
   * @return a real number between 0 and 1
   */
  def transform(value: Any): Double

  def serializeToJson(): String =
    mapper.writeValueAsString(
      Map("type" -> this.getClass.getSimpleName, "value" -> mapper.writeValueAsString(this)))

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  def isSupersededBy(newTransformation: Transformation): Boolean

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   * @return a new Transformation that contains both this and other.
   */
  def merge(other: Transformation): Transformation
}

trait OrdinalTransformation extends Transformation {
  def ordering: Ordering[Any]

}

/**
 * Identity transformation.
 */
object IdentityTransformation extends Transformation {

  @inline
  override def transform(value: Any): Double = value match {
    case v: Number =>
      v.byteValue()
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this
}
