package io.qbeast.spark.delta

import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Tests for DefaultFileIndex.
 */
class DefaultFileIndexTest extends QbeastIntegrationTestSpec {

  private def createTestTable(
      spark: SparkSession,
      path: String,
      size: Int,
      cubeSize: Int = 100,
      staging: Boolean = false): Unit = {
    import scala.collection.JavaConverters._
    val rows = (0 to size).map(i => Row(i, i.toDouble)).toList.asJava
    val schema = StructType(Array(StructField("a", IntegerType), StructField("b", DoubleType)))
    val data = spark.createDataFrame(rows, schema)
    if (staging) {
      data.write.format("delta").mode("overwrite").save(path)
    } else {
      data.write
        .format("qbeast")
        .mode("overwrite")
        .option("columnsToIndex", "a")
        .option("cubeSize", cubeSize)
        .save(path)
    }
  }

  private def newDefaultFileIndex(spark: SparkSession, path: String): DefaultFileIndex = {
    DefaultFileIndex(spark, new Path(path))
  }

  private def newTahoeLogFileIndex(spark: SparkSession, path: String): TahoeLogFileIndex = {
    val log = DeltaLog.forTable(spark, path)
    val snapshot = log.update()
    TahoeLogFileIndex(spark, log, new Path(path), snapshot, Seq.empty, false)
  }

  "DefaultFileIndex" should "use Delta if the query does not have sampling clause" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      createTestTable(spark, tmpDir, 1000, 10)
      val index = newDefaultFileIndex(spark, tmpDir)
      val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
      val filters = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(10)))
      index.listFiles(Seq.empty, filters) shouldBe tahoeIndex.listFiles(Seq.empty, filters)
  }

  it should "use staging area if the query has a sampling clause" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      createTestTable(spark, tmpDir, 1000, 10, staging = true)
      val index = newDefaultFileIndex(spark, tmpDir)
      val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
      val filters = Seq(
        LessThan(AttributeReference("a", IntegerType)(), Literal(10)),
        LessThanOrEqual(
          Literal(Int.MinValue),
          new QbeastMurmur3Hash(Seq(AttributeReference("a", IntegerType)()))),
        LessThan(
          new QbeastMurmur3Hash(Seq(AttributeReference("a", IntegerType)())),
          Literal(Int.MinValue + 1000)))
      index.listFiles(Seq.empty, filters).head.files.length shouldBe <(
        tahoeIndex.getSnapshot.allFiles.count().toInt)
  }

  it should "use Qbeast index if the query has a sampling clause" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      createTestTable(spark, tmpDir, 1000, 10)
      val index = newDefaultFileIndex(spark, tmpDir)
      val filters = Seq(
        LessThan(AttributeReference("a", IntegerType)(), Literal(10)),
        LessThanOrEqual(
          Literal(Int.MinValue),
          new QbeastMurmur3Hash(Seq(AttributeReference("a", IntegerType)()))),
        LessThan(
          new QbeastMurmur3Hash(Seq(AttributeReference("a", IntegerType)())),
          Literal(Int.MinValue + 1000)))
      val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
      val tahoeFilters = Seq(LessThan(AttributeReference("a", IntegerType)(), Literal(10)))
      index.listFiles(Seq.empty, filters).head.files.length shouldBe <(
        tahoeIndex.listFiles(Seq.empty, tahoeFilters).head.files.length)
  }

  it should "get input files from Delta" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir, 10)
    val index = newDefaultFileIndex(spark, tmpDir)
    val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
    index.inputFiles.toSet shouldBe tahoeIndex.inputFiles.toSet
  }

  it should "get root paths from Delta" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir, 10)
    val index = newDefaultFileIndex(spark, tmpDir)
    val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
    index.rootPaths.toSet shouldBe tahoeIndex.rootPaths.toSet
  }

  it should "get size in bytes from Delta" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir, 10)
    val index = newDefaultFileIndex(spark, tmpDir)
    val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
    index.sizeInBytes shouldBe tahoeIndex.sizeInBytes
  }

  it should "get partition schema from Delta" in withSparkAndTmpDir { (spark, tmpDir) =>
    createTestTable(spark, tmpDir, 10)
    val index = newDefaultFileIndex(spark, tmpDir)
    val tahoeIndex = newTahoeLogFileIndex(spark, tmpDir)
    index.partitionSchema shouldBe tahoeIndex.partitionSchema
  }

}
