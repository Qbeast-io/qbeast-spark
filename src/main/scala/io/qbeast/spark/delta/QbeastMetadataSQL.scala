/*
 * Copyright 2021 Qbeast Analytics, S.L.
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
      weight(TagColumns.maxWeight).as("maxWeight"),
      TagColumns.replicated.cast("boolean").as("replicated"),
      TagColumns.revision.cast("bigint").as("revision"),
      TagColumns.elementCount.cast("bigint").as("elementCount"))

}
