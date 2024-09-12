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

import io.qbeast.core.model.OrderedDataType

import scala.collection.Searching._

case class NumericQuantileTransformation(
    approxQuantiles: IndexedSeq[Any],
    orderedDataType: OrderedDataType)
    extends Transformation {
  private def isDefault: Boolean = approxQuantiles == orderedDataType.defaultQuantiles

  override def transform(value: Any): Double = {
    approxQuantiles.zipWithIndex.minBy { case (v, _) => math.abs(v - value) }._2
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case nt @ NumericQuantileTransformation(hist, _) =>
        if (isDefault) !nt.isDefault
        else if (nt.isDefault) false
        else !(approxQuantiles == hist)
      case _ => false
    }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   *   the other transformation
   * @return
   *   a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = other match {
    case _: NumericQuantileTransformation => other
    case _ => this
  }

}
