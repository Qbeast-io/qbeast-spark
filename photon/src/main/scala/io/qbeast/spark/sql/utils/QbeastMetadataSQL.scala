package io.qbeast.spark.sql.utils

import io.qbeast.core.model.{CubeId, NormalizedWeight, Weight}
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

  val qBlock: Seq[Column] = Seq(col("path"),
    col("tags.revision").cast("bigint").as("revision"),
    weight(col("tags.minWeight")).as("minWeight"),
    weight(col("tags.maxWeight")).as("maxWeight"),
    col("tags.state"),
    col("tags.elementCount").cast("bigint").as("elementCount"),
    col("size"),
    col("modificationTime"))


  val qbeastBlock: Column =
    struct(
      col("path"),
      col("size"),
      col("modificationTime"),
      weight(col("tags.minWeight").cast("int")).as("minWeight"),
      weight(col("tags.maxWeight").cast("int")).as("maxWeight"),
        col("tags.state"),
      col("tags.revision").cast("bigint").as("revision"),
      col("tags.elementCount").cast("bigint").as("elementCount"))
}
