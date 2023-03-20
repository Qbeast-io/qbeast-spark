/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses._
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NewRevisionTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  def appendNewRevision(spark: SparkSession, tmpDir: String, multiplier: Int): Unit = {

    val rdd =
      spark.sparkContext.parallelize(
        0.to(100000)
          .map(i =>
            Client3(i * i, s"student-$i", i, (i * 1000 + 123) * multiplier, i * 2567.3432143)))
    val df = spark.createDataFrame(rdd)
    val names = List("age", "val2")
    df.write
      .format("qbeast")
      .mode("append")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)
  }

  "new revision" should "create different revisions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val spaceMultipliers = List(1, 3, 8)
      spaceMultipliers.foreach(i => appendNewRevision(spark, tmpDir, i))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
      val spaceRevisions = qbeastSnapshot.loadAllRevisions

      // Including the staging revision
      spaceRevisions.size shouldBe spaceMultipliers.length + 1

  }

  it should
    "create different index structure for each one" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        appendNewRevision(spark, tmpDir, 1)
        appendNewRevision(spark, tmpDir, 3)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

        val revisions = qbeastSnapshot.loadAllRevisions
        val allWM =
          revisions
            .map(revision =>
              qbeastSnapshot.loadIndexStatus(revision.revisionID).cubeNormalizedWeights)
        allWM.foreach(wm => wm should not be empty)
    }

  it should
    "create different revision on different desired cube size" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val rdd =
            spark.sparkContext.parallelize(
              Seq(
                Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
                Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

          val df = spark.createDataFrame(rdd)

          val names = List("age", "val2")
          val cubeSize = 3000

          df.write
            .format("qbeast")
            .mode("overwrite")
            .options(
              Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

          qbeastSnapshot.loadLatestRevision.desiredCubeSize shouldBe cubeSize

        }
    }

  it should "create different revision when cubeSize changes" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {
      val rdd =
        spark.sparkContext.parallelize(
          Seq(
            Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
            Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2")

      val cubeSize1 = 1
      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize1.toString))
        .save(tmpDir)

      val cubeSize2 = 2
      df.write
        .format("qbeast")
        .mode("append")
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize2.toString))
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

      // Including the staging revision
      qbeastSnapshot.loadAllRevisions.size shouldBe 3
      qbeastSnapshot.loadLatestRevision.desiredCubeSize shouldBe cubeSize2
    })

  it should "create a Revision based on columnStats" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val rdd =
          spark.sparkContext.parallelize(
            Seq(
              Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
              Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

        val df = spark.createDataFrame(rdd)

        val names = List("age")
        // The actual values are contained within the provided min/max
        val stats = """{ "age_min": 0, "age_max": 20 }"""

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "columnStats" -> stats))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
        val transformation = qbeastSnapshot.loadLatestRevision.transformations.head

        qbeastSnapshot.loadLatestRevision.revisionID shouldBe 1
        transformation shouldBe a[LinearTransformation]
        transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
        transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 20

      }
  }

  it should "use the column stats for one column only" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val rdd =
          spark.sparkContext.parallelize(
            Seq(
              Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
              Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

        val df = spark.createDataFrame(rdd)

        val names = List("age,val2")
        val stats = """{ "age_min": 0, "age_max": 20 }"""

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "columnStats" -> stats))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
        val transformation = qbeastSnapshot.loadLatestRevision.transformations.head

        qbeastSnapshot.loadLatestRevision.revisionID shouldBe 1
        transformation shouldBe a[LinearTransformation]
        transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
        transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 20

      }
  }

  it should "write with proper transformation min/max when columnStats are 'invalid'" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val rdd =
          spark.sparkContext.parallelize(
            Seq(
              Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
              Client3(2, s"student-2", 10, 2 * 1000 + 123, 2 * 2567.3432143)))

        val df = spark.createDataFrame(rdd)

        val names = List("age")
        // columnStats are contained within the actual min/max
        val stats = """{ "age_min": 2, "age_max": 5 }"""

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "columnStats" -> stats))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
        val revision = qbeastSnapshot.loadLatestRevision
        val transformation = revision.transformations.head

        revision.revisionID should be > 0L
        transformation shouldBe a[LinearTransformation]
        transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 1
        transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 10

      }
    }

  it should "append with columnStats" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val rdd =
        spark.sparkContext.parallelize(
          Seq(
            Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
            Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

      val df = spark.createDataFrame(rdd)
      val names = List("age")

      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(Map("columnsToIndex" -> names.mkString(",")))
        .save(tmpDir)

      // APPEND with valid columnStats
      val stats = """{ "age_min": 1, "age_max": 100 }"""
      df.write
        .format("qbeast")
        .mode("append")
        .options(Map("columnsToIndex" -> names.mkString(","), "columnStats" -> stats))
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
      val allRevisions = qbeastSnapshot.loadAllRevisions.sortBy(_.revisionID)

      val firstWriteTransformation =
        allRevisions(1).transformations.head.asInstanceOf[LinearTransformation]
      val (firstMin, firstMax) =
        (firstWriteTransformation.minNumber, firstWriteTransformation.maxNumber)

      firstMin shouldBe 1
      firstMax shouldBe 2

      val appendTransformation =
        allRevisions.last.transformations.head.asInstanceOf[LinearTransformation]
      val (appendMin, appendMax) =
        (appendTransformation.minNumber, appendTransformation.maxNumber)

      appendMin shouldBe 1
      appendMax shouldBe 100
    }
  }

  it should "append with 'invalid' columnStats" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val df1 = Seq(
          Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
          Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)).toDF()

        // Creates a Revision with ID = 1
        df1.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", "age")
          .save(tmpDir)

        // APPEND with 'invalid' columnStats: columnStats < appendMinMax
        val df2 = Seq(
          Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
          Client3(2, s"student-2", 20, 2 * 1000 + 123, 2 * 2567.3432143)).toDF()
        val columnStats = """{ "age_min": 1, "age_max": 10 }"""
        df2.write
          .format("qbeast")
          .mode("append")
          .option("columnsToIndex", "age")
          .option("columnStats", columnStats)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
        val allRevisions = qbeastSnapshot.loadAllRevisions.sortBy(_.revisionID)

        val firstWriteTransformation =
          allRevisions(1).transformations.head.asInstanceOf[LinearTransformation]
        val (firstMin, firstMax) =
          (firstWriteTransformation.minNumber, firstWriteTransformation.maxNumber)

        firstMin shouldBe 1
        firstMax shouldBe 2

        val appendTransformation =
          allRevisions.last.transformations.head.asInstanceOf[LinearTransformation]
        val (appendMin, appendMax) =
          (appendTransformation.minNumber, appendTransformation.maxNumber)

        appendMin shouldBe 1
        appendMax shouldBe 20
      }
  }
}
