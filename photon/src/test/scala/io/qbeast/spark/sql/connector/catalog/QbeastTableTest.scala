package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.datasources.OTreePhotonIndex
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class QbeastTableTest extends QbeastTestSpec {

  behavior of "QbeastTableTest"

  it should "retrieve correctly the schema" in withSpark { (spark) =>
    {
      val qbeastTable = QbeastTable("name", spark, Map.empty, qbeastDataPath)
      val originalSchema = spark.read.format("parquet").load(qbeastDataPath).schema
      qbeastTable.schema() shouldBe originalSchema
    }
  }

  it should "output the user specified schema" in withSpark { (spark) =>
    {
      val userSpecifiedSchema =
        spark.read.format("parquet").load(qbeastDataPath).drop("user_id", "product_id") schema
      val qbeastTable =
        QbeastTable("name", spark, Map.empty, qbeastDataPath, Some(userSpecifiedSchema))
      qbeastTable.schema() shouldBe userSpecifiedSchema
    }
  }

  it should "return a QbeastScanBuilder" in withSpark { (spark) =>
    {
      val qbeastTable =
        QbeastTable("name", spark, Map.empty, qbeastDataPath)
      val scanBuilder =
        qbeastTable.newScanBuilder(new CaseInsensitiveStringMap(Map.empty[String, String].asJava))

      scanBuilder shouldBe a[QbeastScanBuilder]
      scanBuilder.asInstanceOf[QbeastScanBuilder].fileIndex shouldBe a[OTreePhotonIndex]
    }
  }

  it should "have only read capabilities" in withSpark { (spark) =>
    {
      val qbeastTable =
        QbeastTable("name", spark, Map.empty, qbeastDataPath)

      qbeastTable.capabilities() shouldBe Set(TableCapability.BATCH_READ).asJava
    }
  }

  it should "should retrieve the name of the table" in withSpark { (spark) =>
    {
      val qbeastTable =
        QbeastTable("name", spark, Map.empty, qbeastDataPath)

      qbeastTable.name shouldBe "name"
    }
  }

}
