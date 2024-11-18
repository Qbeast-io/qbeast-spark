/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.transform

import com.fasterxml.jackson.annotation.JsonTypeInfo

import java.sql.Date
import java.sql.Timestamp
import java.time.Instant

/**
 * Double value transformation.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.CLASS,
  include = JsonTypeInfo.As.PROPERTY,
  property = "className")
trait Transformation extends Serializable {

  /**
   * Converts a real number to a normalized value.
   *
   * @param value
   *   a real number to convert
   * @return
   *   a real number between 0 and 1
   */
  def transform(value: Any): Double

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  def isSupersededBy(newTransformation: Transformation): Boolean

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   *   the other transformation to merge with this one
   * @return
   *   a new Transformation that contains both this and other.
   */
  def merge(other: Transformation): Transformation
}

/**
 * Identity transformation.
 */
@deprecated("Use IdentityTransformation instead", "0.8.0")
case class IdentityToZeroTransformation(identityValue: Any) extends Transformation {

  @inline
  override def transform(value: Any): Double = value match {

    case v: Number if v == identityValue => 0.0
    case v: Timestamp if v == identityValue => 0.0
    case v: Date if v == identityValue => 0.0
    case v: Instant if v == identityValue => 0.0

  }

  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case IdentityToZeroTransformation(newIdValue) => newIdValue != identityValue
      case _ => true
    }

  override def merge(other: Transformation): Transformation = other

}

/**
 * Zero value for nulls transformation.
 */
@deprecated("Use IdentityTransformation instead", "0.8.0")
object NullToZeroTransformation extends Transformation {

  @inline
  override def transform(value: Any): Double = value match {
    case null => 0.0
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = {
    newTransformation match {
      case NullToZeroTransformation => false
      case _ => true
    }
  }

  override def merge(other: Transformation): Transformation = other

}
