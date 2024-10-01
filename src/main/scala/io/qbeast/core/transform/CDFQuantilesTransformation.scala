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

import io.qbeast.core.model.QDataType
import org.apache.spark.annotation.Experimental

import scala.collection.Searching._

/**
 * A type of transformation that converts Any value to a Double between 0 and 1 based on the
 * specified quantiles
 */
@Experimental
trait CDFQuantilesTransformation extends Transformation {

  /**
   * A sequence of quantiles including the extremes at 0 and 1
   */
  val quantiles: IndexedSeq[Any]

  /**
   * The data type of the transformation
   */
  val dataType: QDataType

  /**
   * The ordering of the data type
   *
   * When the type is an OrderedDataType, the ordering is the one defined in the data type. When
   * the type is a StringDataType, the ordering is the one defined in the String class.
   */
  implicit def ordering: Ordering[Any]

  /**
   * Performs any needed mapping to the value before searching it in the quantiles sequence
   *
   * @return
   */
  def mapValue(value: Any): Any

  /**
   * Transforms a value to a Double between 0 and 1
   *
   *   1. Checks if the value is null, if so, returns 0
   *
   * 2. Searches for the value in the quantiles
   *
   * 3. If the value is found, returns the current index divided by the length of the quantiles
   *
   * 4. If the value is not found, returns the relative position of the insertion point
   *
   * WARNING: If the same number is repeated in the quantiles, the transformation will not always
   * select the same index
   * @param value
   *   the value to convert
   * @return
   *   the number between 0 and 1
   */
  override def transform(value: Any): Double = {

    // If the value is null, we return 0
    if (value == null) return 0d
    // Otherwise, we search for the value in the quantiles
    quantiles.search(mapValue(value)) match {
      // First case when the index is found
      case Found(foundIndex) => foundIndex.toDouble / (quantiles.length - 1)
      // When the index is not found, we return the relative position of the insertion point
      case InsertionPoint(insertionPoint) =>
        if (insertionPoint == 0) 0d
        else if (insertionPoint == quantiles.length) 1d
        else (insertionPoint - 1).toDouble / (quantiles.length - 1)
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * The current CDFQuantilesTransformation is superseded by another if
   *   - the new transformation is a CDFQuantilesTransformation
   *   - the ordering of the new transformation is the same as the current one
   *   - the quantiles of the new transformation are non-empty
   *   - the quantiles of the new transformation are different from the current one
   *
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case newT: CDFQuantilesTransformation =>
        // Is superseded by other CDFQuantilesTransformations with different and non-empty quantiles
        this.ordering == newT.ordering && newT.quantiles.nonEmpty && this.quantiles != newT.quantiles
      case _ =>
        // Not superseded by other transformations: Empty, Linear...
        false
    }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * In the case of CDFQuantilesTransformation, the merge would automatically select the new
   * transformation if it's of the same type
   *
   * @param other
   *   the other transformation
   * @return
   *   a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = other match {
    case otherT: CDFQuantilesTransformation => otherT
    case _ => this
  }

}
