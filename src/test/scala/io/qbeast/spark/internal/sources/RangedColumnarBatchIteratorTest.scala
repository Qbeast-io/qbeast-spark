/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.types.DataTypes
import io.qbeast.core.model.RowRange

/**
 * Tests for [[RangedColumnarBatchIterator]].
 */
class RangedColumnarBatchIteratorTest extends AnyFlatSpec with Matchers {

  "RangedColumnarBatchIterator" should "apply the ranges correctly" in {
    val batches = Seq(newBatch(0), newBatch(5), newBatch(10), newBatch(15), newBatch(20)).iterator
    val ranges = Seq(RowRange(7, 9), RowRange(21, 23), RowRange(9, 11))
    val iterator = new RangedColumnarBatchIterator(batches, ranges)

    iterator.hasNext shouldBe true
    var batch = iterator.next()
    batch.numRows() shouldBe 2
    batch.numCols() shouldBe 1
    var column = batch.column(0)
    column.getInt(0) shouldBe 7
    column.getInt(1) shouldBe 8

    iterator.hasNext shouldBe true
    batch = iterator.next()
    batch.numRows() shouldBe 1
    batch.numCols() shouldBe 1
    column = batch.column(0)
    column.getInt(0) shouldBe 9

    iterator.hasNext shouldBe true
    batch = iterator.next()
    batch.numRows() shouldBe 1
    batch.numCols() shouldBe 1
    column = batch.column(0)
    column.getInt(0) shouldBe 10

    iterator.hasNext shouldBe true
    batch = iterator.next()
    batch.numRows() shouldBe 2
    batch.numCols() shouldBe 1
    column = batch.column(0)
    column.getInt(0) shouldBe 21
    column.getInt(1) shouldBe 22

    iterator.hasNext shouldBe false
  }

  private def newBatch(seed: Int): ColumnarBatch = {
    val column = new OnHeapColumnVector(5, DataTypes.IntegerType)
    (0 until 5).foreach(i => column.putInt(i, seed + i))
    new ColumnarBatch(Array(column), 5)
  }

}
