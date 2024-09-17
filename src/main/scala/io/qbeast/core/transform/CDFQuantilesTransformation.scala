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

import org.apache.hadoop.classification.InterfaceStability.Evolving

import scala.collection.Searching._

@Evolving
trait CDFQuantilesTransformation extends Transformation {

  implicit val ordering: Ordering[Any]

  val quantiles: IndexedSeq[Any]

  val defaultNullValue: Any

  def mapValue(value: Any): Any = {
    value match {
      case v: Any => v
      case null => defaultNullValue
    }
  }

  override def transform(value: Any): Double = {
    quantiles.search(mapValue(value)) match {
      // First case when the index is found
      case Found(foundIndex) => foundIndex.toDouble / (quantiles.length - 1)
      // When the index is not found, we return the relative position of the insertion point
      case InsertionPoint(insertionPoint) =>
        if (insertionPoint == 0) 0d
        else if (insertionPoint == quantiles.length + 1) 1d
        else (insertionPoint - 1).toDouble / (quantiles.length - 1)
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * The current CDFQuantilesTransformation is superseded by another if
   *   - the new transformation is a CDFQuantilesTransformation
   *   - the ordering of the new transformation is the same as the current one
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
        this.ordering == newT.ordering && (this.quantiles == newT.quantiles)
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
    case _: CDFQuantilesTransformation => other
    case _ => this
  }

}
