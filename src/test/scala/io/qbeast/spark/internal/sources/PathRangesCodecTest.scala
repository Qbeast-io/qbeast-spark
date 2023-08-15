/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.qbeast.core.model.RowRange

/**
 * Tests for [[PathRangesCodec]]
 */
class PathRangesCodecTest extends AnyFlatSpec with Matchers {

  "PathRangesCodec" should "encode ranges as a fragment" in {
    val ranges = Seq(RowRange(1, 2), RowRange(3, 4), RowRange(5, 6))
    PathRangesCodec.encode("path", ranges) shouldBe "path#1-2,3-4,5-6"
  }

  it should "encode path without ranges correctly" in {
    PathRangesCodec.encode("path", Seq.empty) shouldBe "path"
  }

  it should "decode path without ranges correctly" in {
    val (path, ranges) = PathRangesCodec.decode("path")
    path shouldBe "path"
    ranges.isEmpty shouldBe true
  }

  it should "decode path with ranges correctly" in {
    val (path, ranges) = PathRangesCodec.decode("path#1-2,3-4,5-6")
    path shouldBe "path"
    ranges shouldBe Seq(RowRange(1, 2), RowRange(3, 4), RowRange(5, 6))
  }

  it should "ignore invalid fragment" in {
    val (path, ranges) = PathRangesCodec.decode("path#invalid")
    path shouldBe "path"
    ranges.isEmpty shouldBe true
  }

  it should "ignore invalid range expression" in {
    val (path, ranges) = PathRangesCodec.decode("path#1-2,invalid,5-6")
    path shouldBe "path"
    ranges shouldBe Array(RowRange(1, 2), RowRange(5, 6))
  }

  it should "ignore invalid range bounds" in {
    val (path, ranges) = PathRangesCodec.decode("path#1-2,a-b,5-6")
    path shouldBe "path"
    ranges shouldBe Array(RowRange(1, 2), RowRange(5, 6))
  }

  it should "ignore too many range bounds" in {
    val (path, ranges) = PathRangesCodec.decode("path#1-2,3-4-5,5-6")
    path shouldBe "path"
    ranges shouldBe Array(RowRange(1, 2), RowRange(5, 6))
  }
}
