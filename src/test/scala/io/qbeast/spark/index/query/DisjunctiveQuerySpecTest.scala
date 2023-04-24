package io.qbeast.spark.index.query

import io.qbeast.core.model.{IntegerDataType, QTableID, Revision, Weight, WeightRange}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.expr

class DisjunctiveQuerySpecTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QuerySpecBuilder"

  "QuerySpecBuilder" should "process disjunctive predicates" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr(s"3 <= id OR id < 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpecs.size shouldBe 2

  })

  it should "process disjunctive equality predicates" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr(s"3 == id OR id == 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpecs.size shouldBe 2

  })

  it should "extract each space correctly" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr(s"3 <= id OR id > 10").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    val tFrom = revision.transformations.head.transform(3)
    val firstQuerySpec = querySpecs.head.querySpace

    firstQuerySpec invokePrivate privateFrom() shouldBe Seq(Some(tFrom))
    firstQuerySpec invokePrivate privateTo() shouldBe Seq(None)

    val tFrom2 = revision.transformations.head.transform(10)
    val secondQuerySpec = querySpecs(1).querySpace

    secondQuerySpec invokePrivate privateFrom() shouldBe Seq(Some(tFrom2))
    secondQuerySpec invokePrivate privateTo() shouldBe Seq(None)
  })

  it should "process complex predicates" in withSpark(spark => {
    val transformations =
      Seq(
        LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
        LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType)).toIndexedSeq
    val columnTransformers = Seq(
      Transformer("linear", "id", IntegerDataType),
      Transformer("linear", "age", IntegerDataType)).toIndexedSeq

    val revision = Revision(
      1,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)

    val expression = expr("id > 3 and id <= 7 or age > 6 and age <= 19").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpecs.size shouldBe 2
  })

  it should "process disjunctive predicates with sample" in withSpark(spark => {
    val revision = createRevision()
    val weightRange = WeightRange(Weight(0.0), Weight(0.1))
    val weightFilter = weightFilters(weightRange)
    val expression = expr(s"3 <= id OR id < 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression, weightFilter)).build(revision)

    querySpecs.size shouldBe 2
    querySpecs.foreach(querySpec => querySpec.weightRange shouldBe weightRange)
  })

}
