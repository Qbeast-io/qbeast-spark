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
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.spark.internal.sources.v2.QbeastStagedTableImpl
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCapability.V1_BATCH_WRITE
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.V1Write

import scala.collection.JavaConverters._

class QbeastStagedTableTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "QbeastCatalog" should "return a QbeastStagedTableImpl when provider = qbeast" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      qbeastCatalog
        .stageCreate(
          Identifier.of(Array("default"), "students"),
          columns,
          Array.empty[Transform],
          Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava) shouldBe a[
        QbeastStagedTableImpl]

    })

  "QbeastStagedTable" should "retrieve schema" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      val qbeastStagedTable =
        qbeastCatalog
          .stageCreate(
            Identifier.of(Array("default"), "students"),
            columns,
            Array.empty[Transform],
            Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
          .asInstanceOf[QbeastStagedTableImpl]

      qbeastStagedTable.schema() shouldBe schema
    })

  it should "retrieve name" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val qbeastStagedTable =
      qbeastCatalog
        .stageCreate(
          Identifier.of(Array("default"), "students"),
          columns,
          Array.empty[Transform],
          Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
        .asInstanceOf[QbeastStagedTableImpl]

    qbeastStagedTable.name() shouldBe "students"
  })

  it should "retrieve capabilities" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val qbeastStagedTable =
      qbeastCatalog
        .stageCreate(
          Identifier.of(Array("default"), "students"),
          columns,
          Array.empty[Transform],
          Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
        .asInstanceOf[QbeastStagedTableImpl]

    qbeastStagedTable.capabilities() shouldBe Set(V1_BATCH_WRITE).asJava
  })

  it should "clean path on abort changes" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {

      val qbeastCatalog = createQbeastCatalog(spark)
      val qbeastStagedTable =
        qbeastCatalog
          .stageCreate(
            Identifier.of(Array("default"), "students"),
            columns,
            Array.empty[Transform],
            Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
          .asInstanceOf[QbeastStagedTableImpl]

      qbeastStagedTable.abortStagedChanges()

      val path = new Path(tmpWarehouse)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(new Path(tmpWarehouse + "/students")) shouldBe false

    })

  it should "commit changes" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val qbeastStagedTable =
      qbeastCatalog
        .stageCreate(
          Identifier.of(Array("default"), "students"),
          columns,
          Array.empty[Transform],
          Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
        .asInstanceOf[QbeastStagedTableImpl]

    // Prepare data to the commit
    val dataToCommit = createTestData(spark)

    // We use the write builder to add the data to the commit
    val writeBuilder = qbeastStagedTable
      .newWriteBuilder(fakeLogicalWriteInfo)
      .build()

    writeBuilder shouldBe a[V1Write]
    writeBuilder.asInstanceOf[V1Write].toInsertableRelation.insert(dataToCommit, false)

    // Commit the staged changes
    // This should create the log and write the data
    qbeastStagedTable.commitStagedChanges()

    // Check if the path exists
    val path = new Path(tmpWarehouse)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    fs.exists(new Path(tmpWarehouse + "/students")) shouldBe true

    // Check if the content of the table is correct
    spark.read.table("students").count() shouldBe dataToCommit.count()
    assertSmallDatasetEquality(
      spark.read.table("students"),
      dataToCommit,
      ignoreNullable = true,
      orderedComparison = false)

  })

}
