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
package io.qbeast.utils

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException

class QbeastUtilsTest extends QbeastIntegrationTestSpec {

  "QbeastUtils" should "compute histogram for string columns" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a", "b", "c", "a", "b", "c", "a", "b", "c").toDF("name")
      val hist = QbeastUtils.computeHistogramForColumn(df, "name")

      hist shouldBe "['a','b','c']"
    })

  it should "compute histogram for Int" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df = Seq(1, 2, 3, 1, 2, 3, 1, 2, 3).toDF("age")
    val hist = QbeastUtils.computeHistogramForColumn(df, "age")

    hist shouldBe "['1','2','3']"
  })

  it should "throw error when the column does not exists" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {
      import spark.implicits._
      val df = Seq("a").toDF("name")
      an[AnalysisException] shouldBe thrownBy(
        QbeastUtils.computeHistogramForColumn(df, "non_existing_column"))
    })

}
