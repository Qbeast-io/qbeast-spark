package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.core.transform.{HashTransformation, LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.expr
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuerySpecBuilderTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  private def weightFilters(weightRange: WeightRange): Expression = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("id").expr))
    val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
    And(lessThan, greaterThanOrEqual)
  }

  private def createRevision(minVal: Int = Int.MinValue, maxVal: Int = Int.MaxValue) = {
    val transformations =
      Seq(LinearTransformation(minVal, maxVal, IntegerDataType)).toIndexedSeq
    val columnTransformers = Seq(Transformer("linear", "id", IntegerDataType)).toIndexedSeq

    Revision(
      0,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)
  }

  val privateFrom: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('from)
  val privateTo: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('to)

  "extractWeightRange" should
    "extract all weight range when expressions is empty" in withSpark(spark => {
      val revision = createRevision()
      val weightRange = new QuerySpecBuilder(Seq.empty).build(revision).weightRange

      weightRange shouldBe WeightRange(Weight.MinValue, Weight.MaxValue)
    })

  it should "filter correctly the expressions for weight" in withSpark(spark => {
    val weightRange = WeightRange(Weight(3), Weight(8))
    val expression = weightFilters(weightRange)
    val revision = createRevision()
    val queryWeightRange = new QuerySpecBuilder(Seq(expression)).build(revision).weightRange

    queryWeightRange shouldBe weightRange
  })

  "extractQueryRange" should "extract query range" in withSpark(spark => {
    val (from, to) = (3, 8)
    val expression = expr(s"id >= $from and id < $to").expr
    val revision = createRevision()
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).querySpace
    val tFrom = revision.transformations.head.transform(3)
    val tTo = revision.transformations.head.transform(8)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(tFrom))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(tTo))
  })

  it should "extract query range when equal to" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("id == 3").expr
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).querySpace
    val t = revision.transformations.head.transform(3)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(t))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(t))
  })

  it should "extract query range when is null" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("id is null").expr
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).querySpace
    val t = revision.transformations.head.transform(null)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(t))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(t))
  })

  it should "extract proper query space when column is string" in withSpark(spark => {
    val nameTransformation = HashTransformation()
    val transformations = Seq(nameTransformation).toIndexedSeq
    val columnTransformers = Seq(Transformer("hashing", "name", StringDataType)).toIndexedSeq

    val revision = Revision(
      0,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)

    val (from, to) = ("qbeast", "QBEAST")

    val equalToExpression = expr(s"name == '$from'").expr
    val equalToSpace = new QuerySpecBuilder(Seq(equalToExpression)).build(revision).querySpace
    equalToSpace shouldBe a[QuerySpaceFromTo]

    val rangeExpression = expr(s"name >= '$from' and name < '$to'").expr
    val rangeSpace = new QuerySpecBuilder(Seq(rangeExpression)).build(revision).querySpace
    rangeSpace shouldBe a[AllSpace]
  })

  it should "extract all query range when expressions is empty" in withSpark(spark => {
    val revision = createRevision()
    val querySpace = new QuerySpecBuilder(Seq.empty).build(revision).querySpace

    querySpace shouldBe a[AllSpace]
  })

  it should "not process disjunctive predicates" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr(s"3 <= id OR id < 8").expr
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).querySpace

    querySpace shouldBe a[AllSpace]
  })

  it should "leverage otree index when filtering with GreaterThan and LessThanOrEqual" in withSpark(
    spark => {
      val (f, t) = (3, 8)

      val expressions = Seq(expr(s"id > $f AND id <= $t").expr)
      val revision = createRevision()
      val querySpace = new QuerySpecBuilder(expressions).build(revision).querySpace
      val idTransformation = LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType)

      querySpace invokePrivate privateFrom() shouldBe Seq(Some(idTransformation.transform(f)))
      querySpace invokePrivate privateTo() shouldBe Seq(Some(idTransformation.transform(t)))
    })

  "extractDataFilters" should "extract qbeast filters correctly" in withSpark(spark => {
    val revision = createRevision()
    val rangeExpression = expr(s"id >= 3 OR id < 8").expr
    val weightExpression = weightFilters(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)))
    val expressions = Seq(rangeExpression, weightExpression)
    val querySpecBuilder = new QuerySpecBuilder(expressions)

    querySpecBuilder.extractDataFilters(expressions, revision) shouldBe expressions

  })

  "extractQuerySpace" should "filter Revision properly" in withSpark(_ => {
    // Revision space ranges: [0, 10], [10, 20], [20, 30], [30, 40]
    val revisions = (0 to 3).map(i => createRevision(10 * i, 10 * (i + 1)).copy(revisionID = i))
    val expressions =
      Seq(
        ("id < -1", 0),
        ("id < 9", 1),
        ("id <= 10", 2),
        ("id >= 15", 3),
        ("id >= 10 AND id < 20", 3),
        ("id < 41", 4)).map(tup => (expr(tup._1).expr, tup._2))

    expressions.foreach { case (expression, answer) =>
      revisions.count { revision =>
        val querySpec = new QuerySpecBuilder(expression :: Nil).build(revision)
        querySpec.querySpace match {
          case _: QuerySpaceFromTo => true
          case _: AllSpace => true
          case _: EmptySpace => false
        }
      } shouldBe answer
    }
  })

  it should "retrieve all revisions with no filter expressions" in withSpark(_ => {
    // Revision space ranges: [0, 10], [10, 20], [20, 30], [30, 40]
    val revisions = (0 to 3).map(i => createRevision(10 * i, 10 * (i + 1)).copy(revisionID = i))

    revisions.count { revision =>
      val querySpec = new QuerySpecBuilder(Seq.empty).build(revision)
      querySpec.querySpace match {
        case _: QuerySpaceFromTo => true
        case _: AllSpace => true
        case _: EmptySpace => false
      }
    } shouldBe revisions.size
  })

}
