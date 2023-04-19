package io.qbeast.spark.sql.connector.catalog

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
class QbeastPhotonDatasourceTest extends QbeastTestSpec {

  "Datasource" should "inferSchema properly" in withSparkAndTmpDir{(spark, _) => {

    val qbeastTableProvider = new QbeastPhotonDatasource()
    val options = new CaseInsensitiveStringMap(Map("path" -> qbeastDataPath).asJava)
    val schema = loadRawData(spark).schema

    qbeastTableProvider.inferSchema(options) shouldBe schema
  }}

  it should "load a QbeastTable" in withSparkAndTmpDir { (spark, _) => {

    val qbeastTableProvider = new QbeastPhotonDatasource()
    val options = new CaseInsensitiveStringMap(Map("path" -> qbeastDataPath).asJava)
    val schema = loadRawData(spark).schema


    val table = qbeastTableProvider.getTable(schema, Array.empty, options)
    table shouldBe a[QbeastTable]
  }
  }
}
