package io.qbeast.spark.index

import io.qbeast.model.{IndexStatus, QTableID, TableChanges}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.OTreeAlgorithmTest.Client4
import io.qbeast.spark.index.writer.SparkDataWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Path

class SparkDataWriterTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with PrivateMethodTester {

  /**
   * Get values needed to test a DataWriter.
   * @return QtableID, schema: StructType, qbeastData: DataFrame and TableChanges
   */
  def prepareDataWriter(
      spark: SparkSession,
      path: String,
      size: Int): (QTableID, StructType, DataFrame, TableChanges) = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(0
        .to(size)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

    val tableID = QTableID(path)
    val parameters: Map[String, String] =
      Map("columnsToIndex" -> "age,val2", "desiredCubeSize" -> "1000")
    val indexStatus =
      IndexStatus(SparkRevisionBuilder.createNewRevision(tableID, df, parameters))
    val (qbeastData, tableChanges) = new SparkOTreeManager().index(df, indexStatus)

    (tableID, df.schema, qbeastData, tableChanges)
  }

  "SparkDataWriter" should "write the data correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      val (tableID, schema, qbeastData, tableChanges) =
        prepareDataWriter(spark, tmpDir, 10000)
      val fileActions = new SparkDataWriter().write(tableID, schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }
}
