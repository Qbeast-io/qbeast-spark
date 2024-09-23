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

/**
 * An Placeholder for Transformations that are not yet initialized
 *
 * This Placeholder is NEVER MEANT to be used as a final transformation When tryng to transform a
 * value with the transform method, it will throw an UnsupportedOperationException
 */
case class ManualPlaceholderTransformation(columnName: String, columnStatsNames: Seq[String])
    extends Transformation {

  override def transform(value: Any): Double = {
    throw new UnsupportedOperationException(
      "ManualPlaceholderTransformation does not support transform. " +
        s"Please provide the valid transformation of $columnName through option 'columnStats'")
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = {
    throw new UnsupportedOperationException(
      "ManualPlaceholderTransformation does not support superseeded by. " +
        s"Please provide the valid transformation of $columnName through option 'columnStats'")
  }

  override def merge(other: Transformation): Transformation = {
    throw new UnsupportedOperationException(
      "ManualPlaceholderTransformation does not support merge with other Transformations. " +
        s"Please provide the valid transformation of $columnName through option 'columnStats'")
  }

}
