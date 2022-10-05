package io.qbeast.spark.internal.sources.catalog

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, TableChange}
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

}
