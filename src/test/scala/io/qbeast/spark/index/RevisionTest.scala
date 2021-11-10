/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.Revision
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.QbeastSnapshot
import io.qbeast.spark.index.OTreeAlgorithmTest.{Client3, Client4}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RevisionTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with PrivateMethodTester {

  def writeAndLoadDF(
      df: DataFrame,
      columnsToIndex: String,
      directory: String,
      spark: SparkSession): Revision = {
    df.write
      .mode("error")
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex)
      .save(directory)
    val deltaLog = DeltaLog.forTable(spark, directory)
    val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot)
    val lastRevision = qbeastSnapshot.lastRevision
    val dfqbeast = spark.read.format("qbeast").load(directory)
    dfqbeast.createTempView("dfqbeast")

    lastRevision
  }

  def getMaxMinFromRevision(revision: Revision, spark: SparkSession): Seq[Seq[Any]] = {
    for (t <- revision.columnTransformers)
      yield for (c <- t.stats.columns)
        yield spark.sql(s"SELECT %s FROM dfqbeast".format(c)).collect()(0)(0)
  }

  "LinearTransformer" should
    "get correct values with no nullable numbers" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        val df = spark.createDataFrame(spark.sparkContext.parallelize(
          0.to(100).map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143))))

        val cols = "age,val2,val3"
        val lastRevision = writeAndLoadDF(df, cols, tmpDir, spark)

        getMaxMinFromRevision(lastRevision, spark) shouldBe List(
          List(100, 0),
          List(100123, 123),
          List(256734.32143, 0.0))
    }

  it should "get correct values with nullable numbers" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.createDataFrame(spark.sparkContext.parallelize(0
        .to(100)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

      val cols = "age,val2,val3"
      val lastRevision = writeAndLoadDF(df, cols, tmpDir, spark)

      getMaxMinFromRevision(lastRevision, spark) shouldBe List(
        List(100.0, 0.0),
        List(100123.0, 123.0),
        List(256734.32143, 0.0))
  }

}
