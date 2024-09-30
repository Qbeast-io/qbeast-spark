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
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException

class QbeastUtilsTest extends QbeastIntegrationTestSpec {

  "QbeastUtils" should "compute quantiles for string columns" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a", "b", "c", "a", "b", "c", "a", "b", "c").toDF("name")
      val quantiles = QbeastUtils.computeQuantilesForColumn(df, "name")

      quantiles shouldBe "['a', 'b', 'c']"
    })

  it should "include the extremes in the quantiles for numeric columns" in withQbeastContextSparkAndTmpDir(
    (spark, _s) => {
      val df = spark.range(1, 101).toDF("age")
      val quantiles = QbeastUtils.computeQuantilesForColumn(df, "age", 4, 0.0)

      quantiles shouldBe "[1.0, 25.0, 50.0, 75.0, 100.0]"
    })

  it should "throw error when numberOfQuantiles <= 1" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a", "b", "c", "a", "b", "c", "a", "b", "c").toDF("name")
      an[IllegalArgumentException] shouldBe thrownBy(
        QbeastUtils.computeQuantilesForColumn(df, "name", 1))
      an[IllegalArgumentException] shouldBe thrownBy(
        QbeastUtils.computeQuantilesForColumn(df, "name", 0))
    })

  it should "throw error when the column does not exists" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a").toDF("name")
      an[AnalysisException] shouldBe thrownBy(
        QbeastUtils.computeQuantilesForColumn(df, "non_existing_column"))
    })

}
