/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.NumericQuantileTransformer
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T3
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.to_timestamp

import java.text.SimpleDateFormat

class SparkRevisionFactoryTest extends QbeastIntegrationTestSpec {

  behavior of "SparkRevisionFactory"

  it should "createNewRevision with only one columns" in withSpark(spark => {
    import spark.implicits._
    val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(QbeastOptions.COLUMNS_TO_INDEX -> "a", QbeastOptions.CUBE_SIZE -> "10")))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(LinearTransformer("a", LongDataType))
    revision.transformations shouldBe Vector.empty

  })

  it should "createNewRevision with only indexed columns and no spec" in withSpark(spark => {
    import spark.implicits._
    val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c,d", QbeastOptions.CUBE_SIZE -> "10")))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(
      LinearTransformer("a", LongDataType),
      LinearTransformer("b", DoubleDataType),
      HashTransformer("c", StringDataType),
      LinearTransformer("d", FloatDataType))
    revision.transformations shouldBe Vector.empty

    val revisionExplicit =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(
            QbeastOptions.COLUMNS_TO_INDEX -> "a:linear,b:linear,c:hashing,d:linear",
            QbeastOptions.CUBE_SIZE -> "10")))

    revisionExplicit.copy(timestamp = 0) shouldBe revision.copy(timestamp = 0)
  })

  it should "createNewRevision with min max transformation" in withSpark(spark => {
    import spark.implicits._
    val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(
            QbeastOptions.COLUMNS_TO_INDEX -> "a",
            QbeastOptions.CUBE_SIZE -> "10",
            QbeastOptions.STATS -> """{ "a_min": 0, "a_max": 10 }""")))

    revision.tableID shouldBe qid
    // the reason while it's 1 is because columnStats are provided here
    revision.revisionID shouldBe 1
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(LinearTransformer("a", LongDataType))

    val transformation = revision.transformations.head
    transformation should not be null
    transformation shouldBe a[LinearTransformation]
    transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
    transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 10

  })

  it should "append with new columnStats" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df = spark.range(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f))
    val columnStats = """{ "a_min": 0, "a_max": 20 }"""

    df.write
      .format("qbeast")
      .option("columnsToIndex", "a")
      .option("columnStats", columnStats)
      .save(tmpDir)

    val appendDf = spark.range(10, 21).map(i => T3(i, i * 2.0, s"$i", i * 1.2f))
    val appendColumnStats = """{ "a_min": 5, "a_max": 40 }"""
    appendDf.write
      .mode("append")
      .format("qbeast")
      .option("columnsToIndex", "a")
      .option("columnStats", appendColumnStats)
      .save(tmpDir)

    val qbeastSnapshot =
      DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot)
    val latestRevision = qbeastSnapshot.loadLatestRevision
    val transformation = latestRevision.transformations.head

    transformation should not be null
    transformation shouldBe a[LinearTransformation]
    transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
    transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 40

  })

  it should "createNewRevision with columnStats " +
    "even on APPEND mode" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val df = spark.range(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f))
      val columnStats = """{ "a_min": 0, "a_max": 20 }"""

      // On append mode, it already expects a RevisionChange,
      // but in this case the change is defined by the user
      // instead of triggered by the data

      // TODO: very special case
      // TODO: a cleaner solution would be to change the API for IndexManager
      //  and allow to send a set of options
      //  with user-specific configurations to Index

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "a")
        .option("columnStats", columnStats)
        .save(tmpDir)

      val qbeastSnapshot =
        DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).update())
      val latestRevision = qbeastSnapshot.loadLatestRevision
      val transformation = latestRevision.transformations.head

      transformation should not be null
      transformation shouldBe a[LinearTransformation]
      transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
      transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 20

    })

  it should "createNewRevision with min max timestamp" in withSpark(spark => {
    import spark.implicits._
    val data = Seq(
      "2017-01-03 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-01 12:02:00",
      "2017-01-01 12:02:00")
    val df = data
      .toDF("date")
      .withColumn("date", to_timestamp($"date"))
    val schema = df.schema

    val minTimestamp = df.selectExpr("min(date)").first().getTimestamp(0)
    val maxTimestamp = df.selectExpr("max(date)").first().getTimestamp(0)
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
    val columnStats =
      s"""{ "date_min":"${formatter.format(minTimestamp)}",
         |"date_max":"${formatter.format(maxTimestamp)}" }""".stripMargin

    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(QbeastOptions.COLUMNS_TO_INDEX -> "date", QbeastOptions.STATS -> columnStats)))

    val transformation = revision.transformations.head
    transformation should not be null
    transformation shouldBe a[LinearTransformation]
    transformation.asInstanceOf[LinearTransformation].minNumber shouldBe minTimestamp.getTime
    transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe maxTimestamp.getTime

  })

  it should "createNewRevision with min max transformation in more than one column" in withSpark(
    spark => {
      import spark.implicits._
      val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
      val qid = QTableID("t")
      val revision =
        SparkRevisionFactory.createNewRevision(
          qid,
          schema,
          QbeastOptions(
            Map(
              QbeastOptions.COLUMNS_TO_INDEX -> "a,b",
              QbeastOptions.CUBE_SIZE -> "10",
              QbeastOptions.STATS ->
                """{ "a_min": 0, "a_max": 10, "b_min": 10.0, "b_max": 20.0}""".stripMargin)))

      revision.tableID shouldBe qid
      // the reason while it's 1 is because columnStats are provided here
      revision.revisionID shouldBe 1
      revision.desiredCubeSize shouldBe 10
      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2

      val a_transformation = revision.transformations.head
      a_transformation should not be null
      a_transformation shouldBe a[LinearTransformation]
      a_transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 0
      a_transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 10

      val b_transformation = revision.transformations(1)
      b_transformation should not be null
      b_transformation shouldBe a[LinearTransformation]
      b_transformation.asInstanceOf[LinearTransformation].minNumber shouldBe 10.0
      b_transformation.asInstanceOf[LinearTransformation].maxNumber shouldBe 20.0

    })

  it should "createNewRevision with only indexed columns with all hash" in withSpark(spark => {
    import spark.implicits._
    val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(
            QbeastOptions.COLUMNS_TO_INDEX -> "a:hashing,b:hashing,c:hashing,d:hashing",
            QbeastOptions.CUBE_SIZE -> "10")))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(
      HashTransformer("a", LongDataType),
      HashTransformer("b", DoubleDataType),
      HashTransformer("c", StringDataType),
      HashTransformer("d", FloatDataType))
    revision.transformations shouldBe Vector.empty

  })

  it should "create new revision with approximate percentiles type" in withSpark(spark => {
    import spark.implicits._
    val schema = spark.range(1).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        QbeastOptions(
          Map(QbeastOptions.COLUMNS_TO_INDEX -> "a:percentile", QbeastOptions.CUBE_SIZE -> "10")))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0L
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(NumericQuantileTransformer("a", LongDataType))

  })

}
