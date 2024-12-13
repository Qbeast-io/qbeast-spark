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
package io.qbeast.spark.delta

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

class QbeastSnapshotDeltaTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    spark
      .range(size)
      .map(i => Client3(i * i, s"student-$i", i.intValue(), i * 2, i * i))

  }

  "QbeastSnapshot" should "fail if deletion Vector are enable" in {
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")) {
      (spark, tmpDir) =>
        val nrows = 100
        val df = createDF(nrows)
        val names = List("age", "val2")
        val cubeSize = 100
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .option("enableDeletionVectors", "true")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)
        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        val lastRev = qbeastSnapshot.loadLatestRevision
        val filesToOptimize = qbeastSnapshot.loadIndexFiles(lastRev.revisionID)

        intercept[UnsupportedOperationException] {
          qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize)
        }

    }
  }

  "Appends" should "only update metadata when needed" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(100)
        df.write
          .format("qbeast")
          .option("columnsToIndex", "age,val2")
          .save(tmpDir)
        df.write.mode("append").format("qbeast").save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val conf = deltaLog.newDeltaHadoopConf()
        deltaLog.store
          .read(FileNames.deltaFile(deltaLog.logPath, 0L), conf)
          .map(Action.fromJson)
          .collect { case a: Metadata => a }
          .isEmpty shouldBe false

        deltaLog.store
          .read(FileNames.deltaFile(deltaLog.logPath, 1L), conf)
          .map(Action.fromJson)
          .collect { case a: Metadata => a }
          .isEmpty shouldBe true
      }
    }

}
