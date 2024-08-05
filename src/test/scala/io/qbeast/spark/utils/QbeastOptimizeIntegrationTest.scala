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
import io.qbeast.spark.QbeastTable

class QbeastOptimizeIntegrationTest extends QbeastIntegrationTestSpec {

  "optimize" should "not replicate any data" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = loadTestData(spark)
      writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)
      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize()

      val indexed = spark.read.format("delta").load(tmpDir)
      assertLargeDatasetEquality(indexed, data, orderedComparison = false)
  }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir, "append")

        // Optimize the index
        val qt = QbeastTable.forPath(spark, tmpDir)
        qt.optimize()

        val df = spark.read.format("qbeast").load(tmpDir)
        val dataSize = df.count()
        dataSize shouldBe data.count() * 2

        val tolerance = 0.01
        List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
          val result = df
            .sample(withReplacement = false, precision)
            .count()
            .toDouble

          result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
        })
      }
  }

}
