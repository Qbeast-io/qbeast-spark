package io.qbeast.spark.internal.sources.catalog

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DefaultStagedTableTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "A DefaultStagedTable" should "be returned by default" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      val tableIdentifier = Identifier.of(defaultNamespace, "student")

      qbeastCatalog.stageCreate(
        tableIdentifier,
        schema,
        Array.empty,
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]

      qbeastCatalog.stageReplace(
        tableIdentifier,
        schema,
        Array.empty,
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]

      qbeastCatalog.stageCreateOrReplace(
        tableIdentifier,
        schema,
        Array.empty,
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]
    })

  it should "output table properties" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(tableIdentifier, schema, Array.empty, Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.properties() shouldBe underlyingTable.properties()
  })

  it should "output partitioning" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(tableIdentifier, schema, Array.empty, Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.partitioning() shouldBe underlyingTable.partitioning()
  })

  it should "output capabilities" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(tableIdentifier, schema, Array.empty, Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.capabilities() shouldBe underlyingTable.capabilities()
  })

  it should "output schema" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(tableIdentifier, schema, Array.empty, Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.schema() shouldBe schema
  })

  it should "output name" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(tableIdentifier, schema, Array.empty, Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.name() shouldBe "students"
  })

  it should "drop table on abort" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        schema,
        Array.empty,
        Map.empty[String, String].asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      defaultStagedTable.abortStagedChanges()
      catalog.listTables(defaultNamespace) shouldBe Array()
    })

  it should "throw exception when creating default WriteBuilder" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        schema,
        Array.empty,
        Map.empty[String, String].asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      val logicalWriteInfo = new LogicalWriteInfo {
        override def options(): CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()

        override def queryId(): String = "1"

        override def schema(): StructType = schema
      }

      an[AnalysisException] shouldBe thrownBy(
        defaultStagedTable.newWriteBuilder(logicalWriteInfo))
    })
}
