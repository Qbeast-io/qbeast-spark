package io.qbeast.spark.delta.writer

import io.qbeast.TestClasses._
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory

import scala.reflect.io.Path

class SparkDeltaDataWriterTest extends QbeastIntegrationTestSpec {

  "SparkDataWriter" should "write the data correctly" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      val df = spark.createDataFrame(spark.sparkContext.parallelize(0
        .to(size)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

      val tableID = QTableID(tmpDir)
      val parameters: Map[String, String] =
        Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString)
      val indexStatus =
        IndexStatus(SparkRevisionFactory.createNewRevision(tableID, df.schema, parameters))
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

      val fileActions = SparkDeltaDataWriter.write(tableID, df.schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }

}
