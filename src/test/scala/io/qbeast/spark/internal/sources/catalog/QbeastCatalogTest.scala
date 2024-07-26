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
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogExtension
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.NamespaceChange
import org.apache.spark.sql.connector.catalog.SparkCatalogV2Util
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.AnalysisException

import scala.collection.JavaConverters._

class QbeastCatalogTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "Qbeast catalog" should "create table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
  })

  it should "replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    val newSchema = schema.add(StructField("newCol", IntegerType, false))
    val newColumns = SparkCatalogV2Util.structTypeToV2Columns(newSchema)
    qbeastCatalog.stageReplace(
      tableIdentifier,
      newSchema,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
    qbeastCatalog.loadTable(tableIdentifier).columns() shouldBe newColumns

  })

  it should "create or replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.stageCreateOrReplace(
      tableIdentifier,
      schema,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)

    val newSchema = schema.add(StructField("newCol", IntegerType, false))
    val newColumns = SparkCatalogV2Util.structTypeToV2Columns(newSchema)

    qbeastCatalog.stageCreateOrReplace(
      tableIdentifier,
      newSchema,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
    qbeastCatalog.loadTable(tableIdentifier).columns() shouldBe newColumns
  })

  it should "create table with properties" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map("newProperty" -> "newValue").asJava)

    qbeastCatalog
      .loadTable(Identifier.of(defaultNamespace, "student"))
      .properties()
      .asScala should contain("newProperty" -> "newValue")
  })

  it should "list tables" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    qbeastCatalog.listTables(defaultNamespace) shouldBe Array()
  })

  it should "alter table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    // Alter table with new information
    qbeastCatalog.alterTable(
      tableIdentifier,
      TableChange.addColumn(Array("x"), IntegerType, false))

    val modifiedSchema = StructType(schema.fields ++ Seq(StructField("x", IntegerType, false)))
    val modifiedColumns = SparkCatalogV2Util.structTypeToV2Columns(modifiedSchema)
    qbeastCatalog
      .loadTable(Identifier.of(defaultNamespace, "student"))
      .columns() shouldBe modifiedColumns
  })

  it should "set properties" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)

    val setPropertiesChange = TableChange.setProperty("newProperty", "newValue")
    // Alter table with new information
    qbeastCatalog.alterTable(tableIdentifier, setPropertiesChange)

    qbeastCatalog
      .loadTable(Identifier.of(defaultNamespace, "student"))
      .properties()
      .asScala should contain("newProperty" -> "newValue")
  })

  it should "unset properties" in withQbeastContextSparkAndTmpDir((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map("newProperty" -> "newValue").asJava)

    val unsetPropertiesChange = TableChange.removeProperty("newProperty")
    // Alter table with new information
    qbeastCatalog.alterTable(tableIdentifier, unsetPropertiesChange)

    qbeastCatalog
      .loadTable(Identifier.of(defaultNamespace, "student"))
      .properties()
      .asScala should not contain ("newProperty" -> "newValue")
  })

  it should "drop table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)
    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)

    // Drop table
    qbeastCatalog.dropTable(tableIdentifier)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array()

  })

  it should "rename table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      columns,
      Array.empty[Transform],
      Map.empty[String, String].asJava)
    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)

    // Rename table
    val newTableIdentifier = Identifier.of(defaultNamespace, "new_students")
    qbeastCatalog.renameTable(tableIdentifier, newTableIdentifier)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(newTableIdentifier)
  })

  it should "list all namespaces" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    qbeastCatalog.listNamespaces() shouldBe Array(defaultNamespace)
  })

  it should "create namespace" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    qbeastCatalog.createNamespace(Array("new_namespace"), Map.empty[String, String].asJava)

    qbeastCatalog.listNamespaces() shouldBe Array(defaultNamespace, Array("new_namespace"))
  })

  it should "list specific namespaces" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)

    qbeastCatalog.listNamespaces(defaultNamespace) shouldBe Array()

  })

  it should "load namespace metadata" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpLocation) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      qbeastCatalog.loadNamespaceMetadata(defaultNamespace) shouldBe Map(
        "comment" -> "default database",
        "location" -> ("file:" + tmpLocation),
        "owner" -> scala.util.Properties.userName).asJava
    })

  it should "alter namespace" in withQbeastContextSparkAndTmpWarehouse((spark, tmpLocation) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val newNamespace = Array("new_namespace")
    qbeastCatalog.createNamespace(newNamespace, Map.empty[String, String].asJava)

    // Alter namespace
    qbeastCatalog.alterNamespace(
      newNamespace,
      NamespaceChange.setProperty("newPropertie", "newValue"))

    qbeastCatalog.loadNamespaceMetadata(newNamespace) shouldBe Map(
      "comment" -> "",
      "location" -> ("file:" + tmpLocation + "/new_namespace.db"),
      "owner" -> scala.util.Properties.userName,
      "newPropertie" -> "newValue").asJava

  })

  it should "drop namespace" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val newNamespace = Array("new_namespace")
    qbeastCatalog.createNamespace(newNamespace, Map.empty[String, String].asJava)

    qbeastCatalog.listNamespaces() shouldBe Array(defaultNamespace, Array("new_namespace"))

    // Drop Namespace
    qbeastCatalog.dropNamespace(newNamespace, true)

    qbeastCatalog.listNamespaces() shouldBe Array(defaultNamespace)

  })

  it should "output correct name" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    qbeastCatalog.name() shouldBe null
    qbeastCatalog.initialize("newName", CaseInsensitiveStringMap.empty())
    qbeastCatalog.name() shouldBe "newName"
  })

  it should "throw error when delegating wrong catalog" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      val qbeastCatalog = createQbeastCatalog(spark).asInstanceOf[CatalogExtension]
      val fakeCatalog = new CatalogPlugin {
        override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

        override def name(): String = "fake"
      }

      an[IllegalArgumentException] shouldBe thrownBy(
        qbeastCatalog.setDelegateCatalog(fakeCatalog))
    })

  it should "create a table with PATH" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      val qbeastCatalog = createQbeastCatalog(spark)
      val tableIdentifierPath = Identifier.of(Array("default"), tmpDir + "/student")

      QbeastCatalogUtils.isPathTable(tableIdentifierPath) shouldBe true

      val stagedTable = qbeastCatalog.stageCreate(
        tableIdentifierPath,
        columns,
        Array.empty[Transform],
        Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)

      stagedTable shouldBe an[QbeastStagedTableImpl]
      qbeastCatalog.loadTable(tableIdentifierPath) shouldBe an[QbeastTableImpl]

    })

  "QbeastCatalogUtils" should "throw an error when trying to replace a non-existing table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {
      an[AnalysisException] shouldBe thrownBy(
        QbeastCatalogUtils.createQbeastTable(
          Identifier.of(defaultNamespace, "students"),
          schema,
          Array.empty,
          Map("columnsToIndex" -> "id").asJava,
          Map.empty,
          None,
          TableCreationMode.REPLACE_TABLE,
          indexedTableFactory,
          spark.sessionState.catalog))
    })

  it should "throw an error when inserting data into an external view" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").saveAsTable("students")
      spark.sql("CREATE VIEW students_view AS SELECT * from students")

      an[AnalysisException] shouldBe thrownBy(
        QbeastCatalogUtils
          .getExistingTableIfExists(TableIdentifier("students_view"), spark.sessionState.catalog))
    })

}
