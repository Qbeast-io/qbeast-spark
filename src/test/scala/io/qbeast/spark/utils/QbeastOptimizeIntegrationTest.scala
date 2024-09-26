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

import io.qbeast.core.model.CubeId
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.SparkSession

class QbeastOptimizeIntegrationTest extends QbeastIntegrationTestSpec {

  def optimize(spark: SparkSession, tmpDir: String, times: Int): Unit = {
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    (0 until times).foreach(_ => { qbeastTable.analyze(); qbeastTable.optimize() })

  }

  "the Qbeast data source" should
    "not replicate any point if there are optimizations" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("parquet").load(tmpDir)
          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        }
    }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)
        val dataSize = data.count()

        df.count() shouldBe dataSize

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

  it should "erase cube information when overwritten" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        // val tmpDir = "/tmp/qbeast3"
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)

        // Overwrite table
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)

        qbeastTable.analyze() shouldBe Seq(CubeId.root(2).string)

      }
  }

}
