package io.qbeast.spark.index.writer

import io.qbeast.TestClasses._
import io.qbeast.model.{IndexStatus, QTableID, TableChanges}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.{SparkOTreeManager, SparkRevisionFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.reflect.io.Path

class SparkDataWriterTest extends QbeastIntegrationTestSpec {

  /**
   * Get values needed to test a DataWriter.
   * @return QtableID, schema: StructType, qbeastData: DataFrame and TableChanges
   */
  def prepareDataWriter(
      spark: SparkSession,
      path: String,
      size: Int,
      cubeSize: Int): (QTableID, StructType, DataFrame, TableChanges) = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(0
        .to(size)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

    val tableID = QTableID(path)
    val parameters: Map[String, String] =
      Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString)
    val indexStatus =
      IndexStatus(SparkRevisionFactory.createNewRevision(tableID, df.schema, parameters))
    val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

    (tableID, df.schema, qbeastData, tableChanges)
  }

  "SparkDataWriter" should "write the data correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      val (tableID, schema, qbeastData, tableChanges) =
        prepareDataWriter(spark, tmpDir, 10000, 1000)
      val fileActions = SparkDataWriter.write(tableID, schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }
}
