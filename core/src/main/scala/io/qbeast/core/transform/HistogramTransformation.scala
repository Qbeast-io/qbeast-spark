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

@deprecated("Use CDFQuantilesTransformation instead", "0.8.0")
trait HistogramTransformation extends Transformation {

  /**
   * QDataType for the associated column.
   */
  def dataType: QDataType

  /**
   * Histogram of the associated column that reflects the distribution of the column values.
   * @return
   */
  def histogram: IndexedSeq[Any]

  /**
   * Determines whether the associated histogram is the default one
   * @return
   */
  def isDefault: Boolean

  override def transform(value: Any): Double

  override def isSupersededBy(newTransformation: Transformation): Boolean

  override def merge(other: Transformation): Transformation
}
