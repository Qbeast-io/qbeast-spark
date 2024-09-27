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
package io.qbeast.spark.index

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.StagingUtils
import io.qbeast.TestClasses.T2
import io.qbeast.context.QbeastContext
import io.qbeast.internal.commands.ConvertToQbeastCommand
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.PrivateMethodTester

class DataStagingTest
    extends QbeastIntegrationTestSpec
    with PrivateMethodTester
    with StagingUtils {

  def createDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.range(10000).map(i => T2(i, i.toDouble)).toDF()
  }

  private val getCurrentStagingSize: PrivateMethod[Long] =
    PrivateMethod[Long]('currentStagingSize)

  "Data Staging" should "stage data during first write" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSizeInBytes", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val revisions = getQbeastSnapshot(tmpDir).loadAllRevisions
      revisions.size shouldBe 1
      isStaging(revisions.head) shouldBe true
    }
  }

  it should "not stage data when the staging is full" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSizeInBytes", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val snapshot = getQbeastSnapshot(tmpDir)
      val revisions = snapshot.loadAllRevisions
      revisions.size shouldBe 2

      val stagingDataManager =
        QbeastContext.stagingDataManagerBuilder.getManager(QTableID(tmpDir))

      val indexedDataSize = snapshot
        .loadIndexStatus(1)
        .cubesStatuses
        .values
        .map(_.elementCount)
        .sum

      stagingDataManager invokePrivate getCurrentStagingSize() shouldBe 0L
      indexedDataSize shouldBe 20000L
    }
  }

  it should "clear the staging area by setting spark.qbeast.index.stagingSize=0" in
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.qbeast.index.stagingSizeInBytes", "0")) { (spark, tmpDir) =>
      // Write with delta
      val df = createDF(spark)
      df.write
        .mode("overwrite")
        .format("delta")
        .save(tmpDir)

      // Convert delta files into qbeast staging data
      ConvertToQbeastCommand(s"delta.`$tmpDir`", Seq("a", "c"), 5000).run(spark)

      // Clear the staging area having spark.qbeast.index.stagingSize=0
      import spark.implicits._
      Seq(T2(1, 1))
        .toDF()
        .write
        .mode("append")
        .format("qbeast")
        .save(tmpDir)

      val snapshot = getQbeastSnapshot(tmpDir)
      val revisions = snapshot.loadAllRevisions
      revisions.size shouldBe 2

      val stagingDataManager =
        QbeastContext.stagingDataManagerBuilder.getManager(QTableID(tmpDir))

      val indexedDataSize = snapshot
        .loadIndexStatus(1)
        .cubesStatuses
        .values
        .map(_.elementCount)
        .sum

      stagingDataManager invokePrivate getCurrentStagingSize() shouldBe 0L
      indexedDataSize shouldBe 10001L
    }

}
