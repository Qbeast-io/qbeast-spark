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

import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.seqAsJavaListConverter

class MetadataManagerTest extends QbeastIntegrationTestSpec {

  val dataSchema: StructType = StructType(
    Array(StructField("a", IntegerType), StructField("b", DoubleType)))

  private def createTestTable(spark: SparkSession, path: String, size: Int = 10): Unit = {
    val rows = (0 until size).map(i => Row(i, i.toDouble)).toList.asJava
    val data = spark.createDataFrame(rows, dataSchema)
    data.write.format("qbeast").option("columnsToIndex", "a").save(path)
  }

  "MetadataManager" should "create log directory if it doesn't exist" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      QbeastContext.metadataManager.existsLog(QTableID(tmpDir)) shouldBe false
      QbeastContext.metadataManager.createLog(QTableID(tmpDir))
      QbeastContext.metadataManager.existsLog(QTableID(tmpDir)) shouldBe true
  }

  it should "check if the log exists after table creation" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val tableId = QTableID(tmpDir)
      QbeastContext.metadataManager.existsLog(tableId) shouldBe false
      createTestTable(spark, tmpDir)
      QbeastContext.metadataManager.existsLog(tableId) shouldBe true
  }

  it should "load the snapshot correctly" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir)
    val snapshot = QbeastContext.metadataManager.loadSnapshot(QTableID(tmpDir))
    snapshot.isInitial shouldBe false
  }

  it should "return current schema correctly" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir)
    val schema = QbeastContext.metadataManager.loadCurrentSchema(QTableID(tmpDir))
    schema shouldBe a[StructType]
    schema.fields.length shouldBe 2
    schema shouldBe dataSchema
  }

  it should "update metadata with transaction" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir)
    val tableId = QTableID(tmpDir)

    val snapshot = getQbeastSnapshot(tmpDir)
    snapshot.loadConfiguration shouldNot be(empty)

    // Add new Configuration to the table
    val conf: Map[String, String] = Map("A" -> "1", "B" -> "2")
    QbeastContext.metadataManager.updateMetadataWithTransaction(tableId, dataSchema)(update =
      conf)
    val newSnapshot = getQbeastSnapshot(tmpDir)
    newSnapshot.loadConfiguration should contain allElementsOf conf

    // Remove one key from the config
    val newConfig = newSnapshot.loadConfiguration -- Seq("A")
    QbeastContext.metadataManager.updateMetadataWithTransaction(
      tableId,
      dataSchema,
      overwrite = true)(update = newConfig)
    val updatedSnapshot = getQbeastSnapshot(tmpDir)
    val currentConfig = updatedSnapshot.loadConfiguration
    currentConfig shouldNot contain key "A"
    currentConfig should contain key "B"
    currentConfig("B") shouldBe "2"
  }

}
