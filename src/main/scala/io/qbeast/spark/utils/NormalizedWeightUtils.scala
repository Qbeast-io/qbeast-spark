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
package io.qbeast.spark.utils

import io.qbeast.core.model.Weight
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Column

/**
 * Normalized weight companion object.
 */
object NormalizedWeightUtils {

  def fromColumns(desiredCubeSize: Column, cubeSize: Column): Column = {
    desiredCubeSize.cast(DoubleType) / cubeSize
  }

  def fromWeightColumn(weight: Column): Column = {
    val offset = lit(Weight.offset).cast(DoubleType)
    val range = lit(Weight.range).cast(DoubleType)
    (weight - offset) / range
  }

}
