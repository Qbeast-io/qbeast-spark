package io.qbeast.spark.internal.sources

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.sources.catalog.CatalogTestSuite
import io.qbeast.spark.internal.sources.v2.QbeastStagedTableImpl
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCapability.V1_BATCH_WRITE

import scala.collection.JavaConverters._

class QbeastStagedTableTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "QbeastCatalog" should "return a QbeastStagedTableImpl when provider = qbeast" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      qbeastCatalog
        .stageCreate(
          Identifier.of(Array("default"), "students"),
          schema,
          Array.empty,
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
            schema,
            Array.empty,
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
          schema,
          Array.empty,
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
          schema,
          Array.empty,
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
            schema,
            Array.empty,
            Map("provider" -> "qbeast", "columnsToIndex" -> "id").asJava)
          .asInstanceOf[QbeastStagedTableImpl]

      qbeastStagedTable.abortStagedChanges()

      val path = new Path(tmpWarehouse)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.exists(path.suffix("students")) shouldBe false

    })

}
