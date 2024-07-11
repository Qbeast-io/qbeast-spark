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
import io.qbeast.core.model.QuerySpaceFromTo
import io.qbeast.core.model.Revision
import io.qbeast.core.model.WeightRange
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.TestClasses.T2
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester

trait QueryTestSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {

  lazy val privateFrom: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('from)
  lazy val privateTo: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('to)

  def createDF(size: Int, spark: SparkSession): Dataset[T2] = {
    import spark.implicits._

    spark
      .range(size)
      .map(i => T2(i.toInt, i.toDouble))
      .as[T2]

  }

  def weightFilters(weightRange: WeightRange): Expression = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("a").expr, new Column("c").expr))
    val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
    And(lessThan, greaterThanOrEqual)
  }

  def createRevision(minVal: Int = Int.MinValue, maxVal: Int = Int.MaxValue): Revision = {
    val transformations =
      Seq(LinearTransformation(minVal, maxVal, IntegerDataType)).toIndexedSeq
    val columnTransformers = Seq(Transformer("linear", "id", IntegerDataType)).toIndexedSeq

    Revision(
      1,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)
  }

}
