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

import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.Weight
import io.qbeast.core.model.WeightRange
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.expr

class DisjunctiveQuerySpecTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QuerySpecBuilder"

  "QuerySpecBuilder" should "process disjunctive predicates" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("3 <= id OR id < 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpecs.size shouldBe 2

  })

  it should "process disjunctive equality predicates" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("3 == id OR id == 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpecs.size shouldBe 2

  })

  it should "extract each space correctly" in withSpark(spark => {
    val revision = createRevision()
    val expression = expr("3 <= id OR id > 10").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression)).build(revision)

    // Size of the specs should be one since id > 10 is contained in the expression id >= 3
    querySpecs.size shouldBe 1

    val tFrom = revision.transformations.head.transform(3)
    val querySpace = querySpecs.head.querySpace

    querySpace invokePrivate privateFrom() shouldBe Seq(Some(tFrom))
    querySpace invokePrivate privateTo() shouldBe Seq(None)
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
    val expression = expr("3 <= id OR id < 8").expr
    val querySpecs = new QuerySpecBuilder(Seq(expression, weightFilter)).build(revision)

    querySpecs.size shouldBe 2
    querySpecs.foreach(querySpec => querySpec.weightRange shouldBe weightRange)
  })

}
