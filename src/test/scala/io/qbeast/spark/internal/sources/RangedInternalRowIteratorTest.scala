/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import io.qbeast.core.model.RowRange
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Tests of [[RangedInternalRowIterator]].
 */
class RangedInternalRowIteratorTest extends AnyFlatSpec with Matchers {

  "RangedInternalRowIterator" should "return no rows if there are no ranges" in {
    val rows = Seq(newInternalRow(0)).iterator
    val iterator = new RangedInternalRowIterator(rows, Seq.empty[RowRange])
    iterator.hasNext shouldBe false
  }

  it should "return the rows from ranges if specified" in {
    val rows = Seq(
      newInternalRow(0),
      newInternalRow(1),
      newInternalRow(2),
      newInternalRow(3),
      newInternalRow(4),
      newInternalRow(5)).iterator
    val ranges = Seq(RowRange(1, 2), RowRange(3, 3), RowRange(4, 5))
    val actualRows = new RangedInternalRowIterator(rows, ranges).toVector
    actualRows.length shouldBe 2
    actualRows(0).getInt(0) shouldBe 1
    actualRows(1).getInt(0) shouldBe 4
  }

  it should "return no rows if the underlying iterator is empty" in {
    val rows = Seq.empty[InternalRow].iterator
    val ranges = Seq(RowRange(1, 2), RowRange(3, 3), RowRange(4, 5))
    val iterator = new RangedInternalRowIterator(rows, ranges)
    iterator.hasNext shouldBe false
  }

  private def newInternalRow(value: Int): InternalRow = {
    new GenericInternalRow(Array(value.asInstanceOf[Any]))
  }

}
