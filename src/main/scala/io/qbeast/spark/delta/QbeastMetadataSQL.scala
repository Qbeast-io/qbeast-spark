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
package io.qbeast.spark.delta

import io.qbeast.core.model.{CubeId, NormalizedWeight, Weight}
import io.qbeast.spark.utils.TagColumns
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}

object QbeastMetadataSQL {
  val weight: UserDefinedFunction = udf((weight: Int) => Weight(weight))

  val normalizeWeight: UserDefinedFunction =
    udf((mw: Weight, elementCount: Long, desiredSize: Int) =>
      if (mw < Weight.MaxValue) {
        mw.fraction
      } else {
        NormalizedWeight.apply(desiredSize, elementCount)
      })

  val createCube: UserDefinedFunction =
    udf((cube: String, dimensions: Int) => CubeId(dimensions, cube))

  val qblock: Column =
    struct(
      col("path"),
      TagColumns.cube.as("cube"),
      col("size"),
      col("modificationTime"),
      weight(TagColumns.minWeight).as("minWeight"),
      weight(TagColumns.maxWeight)
        .as("maxWeight"),
      TagColumns.state,
      TagColumns.revision.cast("bigint").as("revision"),
      TagColumns.elementCount.cast("bigint").as("elementCount"))

}
