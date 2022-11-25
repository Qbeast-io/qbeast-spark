package io.qbeast.spark.internal.sources

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.sources.catalog.CatalogTestSuite
import io.qbeast.spark.internal.sources.v2.{QbeastTableImpl, QbeastWriteBuilder}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCapability._

import scala.collection.JavaConverters._

class QbeastTableImplTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  def createQbeastTableImpl(tmpDir: String): QbeastTableImpl = {
    new QbeastTableImpl(
      TableIdentifier("students"),
      new Path(tmpDir),
      Map.empty,
      Some(schema),
      None,
      indexedTableFactory)

  }

  "QbeastTableImpl" should "load the schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.schema() shouldBe schema

    })

  it should "get the name" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {

      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.name() shouldBe "students"

    })

  it should "load capabilities" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.capabilities() shouldBe Set(
        ACCEPT_ANY_SCHEMA,
        BATCH_READ,
        V1_BATCH_WRITE,
        OVERWRITE_BY_FILTER,
        TRUNCATE).asJava
    })

  it should "create a QbeastWriteBuilder" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val qbeastTableImpl = createQbeastTableImpl(tmpDir)
      qbeastTableImpl.newWriteBuilder(fakeLogicalWriteInfo) shouldBe a[QbeastWriteBuilder]
    })

  it should "load properties" in
    withQbeastContextSparkAndTmpWarehouse((_, tmpDir) => {
      val properties = Map("provider" -> "qbeast", "columnsToIndex" -> "id")
      val qbeastTableImpl = new QbeastTableImpl(
        TableIdentifier("students"),
        new Path(tmpDir),
        properties,
        Some(schema),
        None,
        indexedTableFactory)

      qbeastTableImpl.properties() shouldBe properties.asJava

    })

  it should "load metadata from the Catalog" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val qbeastCatalog = createQbeastCatalog(spark)
      val identifier = Identifier.of(defaultNamespace, "students")
      val tableIdentifier = TableIdentifier(identifier.name(), identifier.namespace().headOption)
      val properties = Map.empty[String, String]
      qbeastCatalog.createTable(identifier, schema, Array.empty, properties.asJava)

      val qbeastTableImpl = new QbeastTableImpl(
        tableIdentifier,
        new Path(tmpDir),
        properties,
        None,
        None,
        indexedTableFactory)

      qbeastTableImpl.schema() shouldBe schema
      qbeastTableImpl.v1Table shouldBe spark.sessionState.catalog.getTableMetadata(
        tableIdentifier)
    })

}
