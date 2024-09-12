package io.qbeast.spark.table

import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta.StagingDataManager
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexWithoutRewritingTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "indexWithoutRewriting" should "work with tables converted" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._
      // event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
      val data = loadTestData(spark)
      data.write.format("delta").mode("overwrite").save(tmpDir)
      val sm = new StagingDataManager(QTableID(tmpDir))
      import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
      ConvertToQbeastCommand(
        s"delta.`$tmpDir`",
        Seq("event_time", "event_type", "product_id"),
        10000).run(spark)
      sm.stagingFiles().count() should be > 0L

      val dataWritten = spark.read.format("qbeast").load(tmpDir)
      val files = dataWritten.select(input_file_name()).distinct().as[String].collect().toSet
      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.indexWithoutRewriting(files)
      qt.latestRevision.revisionID shouldBe 1
      val dnb = qt.getDenormalizedBlocks()
      dnb.show() // TODO: denormalized blocks are empty
      println(qt.getIndexMetrics())
      dnb.map(_.cubeId).distinct().count() should be > 1L

      sm.stagingFiles().count() shouldBe 0
  }

}
