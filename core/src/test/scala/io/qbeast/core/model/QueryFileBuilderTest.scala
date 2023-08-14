package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[QueryFileBuilder]].
 */
class QueryFileBuilderTest extends AnyFlatSpec with Matchers {

  "QueryFileBuilder" should "create QueryFile correctly" in {
    val file = File("path", 1, 2)
    val range1 = RowRange(0, 1)
    val range2 = RowRange(1, 2)
    val block =
      Block(file, RowRange(2, 3), CubeId.root(1), "FLOODED", Weight.MinValue, Weight.MaxValue)
    val queryFile =
      new QueryFileBuilder(file).addRange(range1).addRange(range2).addBlock(block).result()
    queryFile.file shouldBe file
    queryFile.ranges.length shouldBe 3
    queryFile.ranges should contain(range1)
    queryFile.ranges should contain(range2)
    queryFile.ranges should contain(block.range)
  }
}
