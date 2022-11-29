package io.qbeast.spark.internal.sources.catalog

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.sources.v2.{QbeastStagedTableImpl, QbeastTableImpl}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.connector.catalog.{
  CatalogExtension,
  CatalogPlugin,
  Identifier,
  NamespaceChange,
  TableChange
}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class QbeastCatalogTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "Qbeast catalog" should "create table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.createTable(
      tableIdentifier,
      schema,
      Array.empty,
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
  })

  it should "replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.createTable(
      tableIdentifier,
      schema,
      Array.empty,
      Map.empty[String, String].asJava)

    val newSchema = schema.add(StructField("newCol", IntegerType, false))
    qbeastCatalog.stageReplace(
      tableIdentifier,
      newSchema,
      Array.empty,
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
    qbeastCatalog.loadTable(tableIdentifier).schema() shouldBe newSchema

  })

  it should "create or replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")

    qbeastCatalog.stageCreateOrReplace(
      tableIdentifier,
      schema,
      Array.empty,
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)

    val newSchema = schema.add(StructField("newCol", IntegerType, false))
    qbeastCatalog.stageCreateOrReplace(
      tableIdentifier,
      newSchema,
      Array.empty,
      Map.empty[String, String].asJava)

    qbeastCatalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
    qbeastCatalog.loadTable(tableIdentifier).schema() shouldBe newSchema
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
      schema,
      Array.empty,
      Map.empty[String, String].asJava)

    // Alter table with new information
    qbeastCatalog.alterTable(
      tableIdentifier,
      TableChange.addColumn(Array("x"), IntegerType, false))

    val modifiedSchema = StructType(schema.fields ++ Seq(StructField("x", IntegerType, false)))
    qbeastCatalog
      .loadTable(Identifier.of(defaultNamespace, "student"))
      .schema() shouldBe modifiedSchema
  })

  it should "drop table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val tableIdentifier = Identifier.of(defaultNamespace, "student")
    qbeastCatalog.createTable(
      tableIdentifier,
      schema,
      Array.empty,
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
      schema,
      Array.empty,
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
        "location" -> ("file:" + tmpLocation)).asJava
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
      "newPropertie" -> "newValue").asJava

  })

  it should "drop namespace" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val qbeastCatalog = createQbeastCatalog(spark)
    val newNamespace = Array("new_namespace")
    qbeastCatalog.createNamespace(newNamespace, Map.empty[String, String].asJava)

    qbeastCatalog.listNamespaces() shouldBe Array(defaultNamespace, Array("new_namespace"))

    // Drop Namespace
    qbeastCatalog.dropNamespace(newNamespace)

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
        schema,
        Array.empty,
        Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)

      stagedTable shouldBe an[QbeastStagedTableImpl]
      qbeastCatalog.loadTable(tableIdentifierPath) shouldBe an[QbeastTableImpl]

    })

  "QbeastCatalogUtils" should "throw an error when trying to replace a non-existing table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {
      an[CannotReplaceMissingTableException] shouldBe thrownBy(
        QbeastCatalogUtils.createQbeastTable(
          Identifier.of(defaultNamespace, "students"),
          schema,
          Array.empty,
          Map.empty[String, String].asJava,
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
