/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index.query

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.core.model._
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.functions.expr

class QuerySpecBuilderTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QuerySpecBuilder"

  "extractWeightRange" should
    "extract all weight range when expressions is empty" in withSpark(spark => {
      val revision = createRevision()
      val weightRange = new QuerySpecBuilder(Seq.empty).build(revision).head.weightRange

      weightRange shouldBe WeightRange(Weight.MinValue, Weight.MaxValue)
    })

  it should "filter correctly the expressions for weight" in withSpark(spark => {
    val weightRange = WeightRange(Weight(3), Weight(8))
    val expression = weightFilters(weightRange)
    val revision = createRevision()
    val queryWeightRange = new QuerySpecBuilder(Seq(expression)).build(revision).head.weightRange

    queryWeightRange shouldBe weightRange
  })

  "extractQueryRange" should "extract query range" in withSpark(spark => {
    val (from, to) = (3, 8)
    val expression = expr(s"id >= $from and id < $to").expr
    val revision = createRevision()
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).head.querySpace

    val tFrom = revision.transformations.head.transform(3)
    val tTo = revision.transformations.head.transform(8)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(tFrom))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(tTo))
  })

  it should "extract query range when equal to" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("id == 3").expr
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).head.querySpace
    val t = revision.transformations.head.transform(3)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(t))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(t))
  })

  it should "extract query range when is null" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("id is null").expr
    val querySpace = new QuerySpecBuilder(Seq(expression)).build(revision).head.querySpace
    val t = revision.transformations.head.transform(null)

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(t))
    querySpace invokePrivate privateTo() shouldBe Seq(Some(t))
  })

  it should "extract proper query space when column is string" in withSpark(spark => {
    val nameTransformation = HashTransformation()
    val transformations = Seq(nameTransformation).toIndexedSeq
    val columnTransformers = Seq(Transformer("hashing", "name", StringDataType)).toIndexedSeq

    val revision = Revision(
      1,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)

    val (from, to) = ("qbeast", "QBEAST")

    val equalToExpression = expr(s"name == '$from'").expr
    val equalToSpace =
      new QuerySpecBuilder(Seq(equalToExpression)).build(revision).head.querySpace
    equalToSpace shouldBe a[QuerySpaceFromTo]

    val rangeExpression = expr(s"name >= '$from' and name < '$to'").expr
    val rangeSpace = new QuerySpecBuilder(Seq(rangeExpression)).build(revision).head.querySpace
    rangeSpace shouldBe a[AllSpace]
  })

  it should "extract all query range when expressions is empty" in withSpark(spark => {
    val revision = createRevision()
    val querySpace = new QuerySpecBuilder(Seq.empty).build(revision).head.querySpace

    querySpace shouldBe a[AllSpace]
  })

  it should "leverage otree index when filtering with GreaterThan and LessThanOrEqual" in withSpark(
    spark => {
      val (f, t) = (3, 8)

      val expressions = Seq(expr(s"id > $f AND id <= $t").expr)
      val revision = createRevision()
      val querySpace = new QuerySpecBuilder(expressions).build(revision).head.querySpace
      val idTransformation = LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType)

      querySpace invokePrivate privateFrom() shouldBe Seq(Some(idTransformation.transform(f)))
      querySpace invokePrivate privateTo() shouldBe Seq(Some(idTransformation.transform(t)))
    })

  "extractQbeastFilters" should "extract qbeast filters correctly" in withSpark(spark => {
    val revision = createRevision()
    val rangeExpression = expr("id >= 3 OR id < 8").expr
    val weightExpression = weightFilters(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)))
    val expressions = Seq(rangeExpression, weightExpression)
    val querySpecBuilder = new QuerySpecBuilder(expressions)

    querySpecBuilder.extractQbeastFilters(expressions, revision) shouldBe QbeastFilters(
      weightExpression.children,
      Seq(rangeExpression))

  })

  "extractQuerySpace" should "filter Revision properly" in withSpark(_ => {
    // Revision space ranges: [0, 10], [10, 20], [20, 30], [30, 40]
    val revisions =
      (0 to 3).map(i => createRevision(10 * i, 10 * (i + 1)).copy(revisionID = i + 1))
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
        val querySpec = new QuerySpecBuilder(expression :: Nil).build(revision).head
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
    val revisions =
      (0 to 3).map(i => createRevision(10 * i, 10 * (i + 1)).copy(revisionID = i + 1))

    revisions.count { revision =>
      val querySpec = new QuerySpecBuilder(Seq.empty).build(revision).head
      querySpec.querySpace match {
        case _: QuerySpaceFromTo => true
        case _: AllSpace => true
        case _: EmptySpace => false
      }
    } shouldBe revisions.size
  })

}
