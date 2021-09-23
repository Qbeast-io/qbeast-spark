/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[ColumnsToIndex]].
 */
class ColumnsToIndexTest extends AnyFlatSpec with Matchers {
  "ColumnsToIndex" should "encode and decode columns correctly" in {
    for (columns <- Seq(Seq("A", "B", "C"), Seq("A"))) {
      ColumnsToIndex.decode(ColumnsToIndex.encode(columns)) shouldBe columns
    }
  }

  it should "implement areSame correctly" in {
    ColumnsToIndex.areSame(Seq("A", "B"), Seq("B", "A")) shouldBe true
    ColumnsToIndex.areSame(Seq("A", "B"), Seq("A")) shouldBe false
  }
}
