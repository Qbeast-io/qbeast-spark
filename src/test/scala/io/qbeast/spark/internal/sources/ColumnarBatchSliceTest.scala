/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Tests for [[ColumnarBatchSlice]].
 */
class ColumnarBatchSliceTest extends AnyFlatSpec with Matchers {

  "ColumnarBatchSlice" should "apply row range to a given target" in {
    val targetColumn = new OnHeapColumnVector(4, DataTypes.IntegerType)
    targetColumn.putInt(0, 0)
    targetColumn.putInt(1, 1)
    targetColumn.putInt(2, 2)
    targetColumn.putInt(3, 3)
    val target = new ColumnarBatch(Array(targetColumn), 4)

    val slice = ColumnarBatchSlice(target, 1, 3)
    slice.numCols() shouldBe target.numCols()
    slice.numRows() shouldBe 2
    slice.getRow(0).getInt(0) shouldBe 1
    slice.getRow(1).getInt(0) shouldBe 2
    slice.column(0).getInt(0) shouldBe 1
    slice.column(0).getInt(1) shouldBe 2
  }

}
