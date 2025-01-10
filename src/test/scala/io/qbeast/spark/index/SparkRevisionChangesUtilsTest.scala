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

import io.qbeast.core.model.DecimalDataType
import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.FloatDataType
import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.COLUMN_STATS
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.model.TimestampDataType
import io.qbeast.core.transform.CDFNumericQuantilesTransformation
import io.qbeast.core.transform.CDFNumericQuantilesTransformer
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T3
import io.qbeast.TestClasses.TestAll
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat

class SparkRevisionChangesUtilsTest
    extends QbeastIntegrationTestSpec
    with SparkRevisionChangesUtils
    with StagingUtils {

  def createData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // a_min = -100, a_max = 100, b_min = -200.0, b_max = 200.0
    // c_min = 1.0, c_max = 2.0, d_min = 1.0, d_max = 2.0
    Seq(T3(-100L, -200.0, "a", 1.0f), T3(100L, 200.0, "b", 2.0f)).toDF
  }

  // Transformation changes (no columnStats)
  // 1. it should create a new revision by adding data to an empty revision (existingSpace = 0 < dataStats)
  // 2. it should create a new revision by adding data to a staging revision (existingSpace = 0 < dataStats)
  // 3. it should create a new revision by adding data to an existing, smaller revision (existingSpace < dataStats)
  // 4. it should not create a new revision by adding data to an existing, larger revision (existingSpace > dataStats)

  // Transformation changes (with columnStats)
  // 5. it should create a new revision by adding data to an empty revision using a larger columnStats (existingSpace = 0 < dataStats < columnStats)
  // 6. it should create a new revision by adding data to an empty revision using a smaller columnStats (existingSpace = 0 < columnStats < dataStats)
  // 7. it should create a new revision by adding data to a staging revision using a larger columnStats (existingSpace = 0 < dataStats < columnStats)
  // 8. it should create a new revision by adding data to a staging revision using a smaller columnStats (existingSpace = 0 < columnStats < dataStats)
  // 9. it should create a new revision by adding data to an existing, smaller revision using a larger columnStats (existingSpace < dataStats < columnStats)
  // 10. it should create a new revision by adding data to an existing, smaller revision using a smaller columnStats (existingSpace < columnStats < dataStats)
  // 11. it should not create a new revision by adding data to an existing revision if (dataStats < columnStats < existingSpace)
  // 12. it should not create a new revision by adding data to an existing revision if (columnStats < dataStats < existingSpace)

  // 13. it should be able to accept columnStats for only some of the indexing columns
  // 14. it should be able to accept columnStats for only some of the indexing columns when doing appends
  // 15. it should be able to accept columnStats for Timestamp columns
  // 16. it should be able to accept columnStats for any Numeric column (Int, Float, Double, Long and Decimal)

  // Transformer changes
  // 17. it should throw an exception when trying to change indexing columns
  // 18. it should change transformer type only when specified
  // 19. it should create a new revision by changing the transformer type between hashing to linear
  // 20. it should create a new revision by changing the transformer type between linear and quantiles
  // 21. it should create a new revision by changing the transformer type between hashing and quantiles
  // 22. it should throw an exception when using a quantile transformation without columnStats
  // 23. it should not create a new revision when appending to an existing quantile transformation without columnStats
  // 24. it should throw an exception when the provided columnStats is not a valid JSON string

  // cubeSize changes
  // 24. it should detect cubeSize changes

  // Transformation changes (no columnStats)
  "SparkRevisionChangesUtils" should
    "create a new revision by adding data to an empty revision (existingSpace = 0 < dataStats)" in
    withSpark { spark =>
      val data = createData(spark)
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val emptyRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val revision = computeRevisionChanges(emptyRevision, options, data)._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to a staging revision (existingSpace = 0 < dataStats)" in
    withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val stagingRev =
        stagingRevision(QTableID("test"), options.cubeSize, options.columnsToIndex)
      val revision =
        computeRevisionChanges(stagingRev, options, createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to an existing, smaller revision (existingSpace < dataStats)" in
    withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val smallerRevision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-1L, 1L, "a", LongDataType),
            LinearTransformation(-2.0, 2.0, "b", DoubleDataType)))
      val revision = computeRevisionChanges(
        smallerRevision,
        options,
        createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "not create a new revision by adding data to an existing, larger revision (existingSpace > dataStats)" in
    withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val largerRevision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-1000L, 1000L, "a", LongDataType),
            LinearTransformation(-2000.0, 2000.0, "b", DoubleDataType)))

      computeRevisionChanges(largerRevision, options, createData(spark))._1 shouldBe None
    }

  // Transformation changes (with columnStats)
  it should
    "create a new revision by adding data to an empty revision and using a larger columnStats " +
    "(existingSpace = 0 < dataStats < columnStats)" in withSpark { spark =>
      val data = createData(spark)
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val emptyRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val revision = computeRevisionChanges(
        emptyRevision,
        options.copy(columnStats =
          Some("""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}""")),
        data)._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to an empty revision and using a smaller columnStats " +
    "(existingSpace = 0 < columnStats < dataStats)" in withSpark { spark =>
      val data = createData(spark)
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val emptyRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val revision = computeRevisionChanges(
        emptyRevision,
        options.copy(columnStats =
          Some("""{"a_min":-10,"a_max":10, "b_min":-20.0, "b_max":20.0}""")),
        data)._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to a staging revision and using a larger columnStats " +
    "(existingSpace = 0 < dataStats < columnStats)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val stagingRev =
        stagingRevision(QTableID("test"), options.cubeSize, options.columnsToIndex)
      val revision = computeRevisionChanges(
        stagingRev,
        options.copy(columnStats =
          Some("""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}""")),
        createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to a staging revision with a smaller columnStats " +
    "(existingSpace = 0 < columnStats < dataStats)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val stagingRev =
        stagingRevision(QTableID("test"), options.cubeSize, options.columnsToIndex)
      val revision = computeRevisionChanges(
        stagingRev,
        options.copy(columnStats =
          Some("""{"a_min":-10,"a_max":10, "b_min":-20.0, "b_max":20.0}""")),
        createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to an existing, smaller revision using a larger columnStats " +
    "(existingSpace < dataStats < columnStats)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val smallerRevision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-1L, 1L, "a", LongDataType),
            LinearTransformation(-2.0, 2.0, "b", DoubleDataType)))
      val revision = computeRevisionChanges(
        smallerRevision,
        options.copy(columnStats =
          Some("""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}""")),
        createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
      }
    }

  it should "create a new revision by adding data to an existing, smaller revision using a smaller columnStats " +
    "(existingSpace < columnStats < dataStats)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val smallerRevision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-1L, 1L, "a", LongDataType),
            LinearTransformation(-2.0, 2.0, "b", DoubleDataType)))
      val revision = computeRevisionChanges(
        smallerRevision,
        options.copy(columnStats =
          Some("""{"a_min":-10,"a_max":10, "b_min":-20.0, "b_max":20.0}""")),
        createData(spark))._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "not create a new revision by adding data to an existing revision if " +
    "(dataStats < columnStats < existingSpace)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val revision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-10000L, 10000L, "a", LongDataType),
            LinearTransformation(-20000.0, 20000.0, "b", DoubleDataType)))
      computeRevisionChanges(
        revision,
        options.copy(columnStats =
          Some("""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}""")),
        createData(spark))._1 shouldBe None
    }

  it should "not create a new revision by adding data to an existing revision if " +
    "(columnStats < dataStats < existingSpace)" in withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val revision =
        Revision(
          1L,
          System.currentTimeMillis(),
          QTableID("test"),
          options.cubeSize,
          Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
          Vector(
            LinearTransformation(-1000L, 1000L, "a", LongDataType),
            LinearTransformation(-2000.0, 2000.0, "b", DoubleDataType)))
      computeRevisionChanges(
        revision,
        options.copy(columnStats =
          Some("""{"a_min":-10,"a_max":10, "b_min":-20.0, "b_max":20.0}""")),
        createData(spark))._1 shouldBe None
    }

  it should "be able to accept columnStats for only some of the indexing columns" in
    withSpark { spark =>
      val data = createData(spark)
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val emptyRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val revision =
        computeRevisionChanges(
          emptyRevision,
          options.copy(columnStats = Some("""{"a_min":-1000,"a_max":1000}""")),
          data)._1.get.createNewRevision

      revision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision.transformations.size shouldBe 2
      revision.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "be able to accept columnStats for only some of the indexing columns when doing appends" in
    withSpark { spark =>
      val data = createData(spark)
      val options1 = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val emptyRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options1)
      val revision1 = computeRevisionChanges(
        emptyRevision,
        options1.copy(columnStats = Some("""{"a_min":-1000,"a_max":1000}""")),
        data)._1.get.createNewRevision

      revision1.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision1.transformations.size shouldBe 2
      revision1.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision1.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }

      val options_2 = QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> """{"b_min":-2000.0, "b_max":2000.0}"""))
      val revision2 =
        computeRevisionChanges(revision1, options_2, data)._1.get.createNewRevision

      revision2.revisionID shouldBe revision1.revisionID + 1
      revision2.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      revision2.transformations.size shouldBe 2
      revision2.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      revision2.transformations.last should matchPattern {
        case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
      }
    }

  it should "be able to accept columnStats for Timestamp columns" in withSpark { spark =>
    import spark.implicits._

    val data = Seq(
      "2017-01-03 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-02 12:02:00",
      "2017-01-01 12:02:00",
      "2017-01-01 12:02:00")
      .toDF("date")
      .withColumn("date", to_timestamp($"date"))
    val minTimestamp = data.selectExpr("min(date)").first().getTimestamp(0)
    val maxTimestamp = data.selectExpr("max(date)").first().getTimestamp(0)

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
    val columnStats = s"""{
                      "date_min":"${formatter.format(minTimestamp)}",
                      "date_max":"${formatter.format(maxTimestamp)}"
                      }""".stripMargin
    val options = QbeastOptions(
      Map(
        QbeastOptions.COLUMNS_TO_INDEX -> "date",
        CUBE_SIZE -> "1000",
        COLUMN_STATS -> columnStats))
    val revision = SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
    val newRevision = computeRevisionChanges(revision, options, data)._1.get.createNewRevision

    newRevision.transformations.size shouldBe 1
    newRevision.transformations.head match {
      case LinearTransformation(minNumber, maxNumber, _, TimestampDataType) =>
        minNumber shouldBe minTimestamp.getTime
        maxNumber shouldBe maxTimestamp.getTime
      case _ => fail("Expected LinearTransformation")
    }
  }

  // Transformer changes
  it should "throw an exception when trying to change indexing columns" in withSpark { spark =>
    val data = createData(spark)
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
    val revision = SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
    val newOptions = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,c", CUBE_SIZE -> "1000"))
    an[AnalysisException] shouldBe thrownBy(computeRevisionChanges(revision, newOptions, data))
  }

  it should "change transformer type only when specified" in withSpark { spark =>
    val schema = createData(spark).schema
    // CDFNumericQuantilesTransformer is not the default transformer for the column
    // Appending with the same column without specifying the transformer should not
    // change the transformer
    val noChanges = computeTransformerChanges(
      Vector(CDFNumericQuantilesTransformer("b", DoubleDataType)),
      QbeastOptions(Seq("b"), 1000, "some_format"),
      schema).flatten

    noChanges.isEmpty shouldBe true

    // Here the HashTransformer is specified, so the transformer should change
    val withChanges = computeTransformerChanges(
      Vector(CDFNumericQuantilesTransformer("b", DoubleDataType)),
      QbeastOptions(Seq("b:hashing"), 1000, "some_format"),
      schema).flatten
    withChanges shouldBe Vector(HashTransformer("b", DoubleDataType))
  }

  it should "create a new revision changing Transformer types between hashing to linear" in withSpark {
    spark =>
      // a: linear -> hashing, b: hashing -> linear
      val revision = Revision.firstRevision(
        QTableID("test"),
        1000,
        Vector(LinearTransformer("a", LongDataType), HashTransformer("b", DoubleDataType)),
        Vector(LinearTransformation(-100L, 100L, LongDataType), HashTransformation()))
      val options =
        QbeastOptions(Map(COLUMNS_TO_INDEX -> "a:hashing,b:linear", CUBE_SIZE -> "1000"))
      val newRevision =
        computeRevisionChanges(revision, options, createData(spark))._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        HashTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head shouldBe a[HashTransformation]
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
  }

  it should "create a new revision by changing Transformer types between linear and quantiles" in withSpark {
    spark =>
      // a: linear -> quantiles, b: quantiles -> linear
      val revision = Revision.firstRevision(
        QTableID("test"),
        1000,
        Vector(
          LinearTransformer("a", LongDataType),
          CDFNumericQuantilesTransformer("b", DoubleDataType)),
        Vector(
          LinearTransformation(-100L, 100L, LongDataType),
          CDFNumericQuantilesTransformation(Vector(0d, 0.5, 1.0), DoubleDataType)))
      val options =
        QbeastOptions(
          Map(
            COLUMNS_TO_INDEX -> "a:quantiles,b:linear",
            CUBE_SIZE -> "1000",
            COLUMN_STATS -> """{"a_quantiles":[0.0, 0.3, 0.6, 1.0]}"""))
      val newRevision =
        computeRevisionChanges(revision, options, createData(spark))._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        CDFNumericQuantilesTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head shouldBe CDFNumericQuantilesTransformation(
        Vector(0.0, 0.3, 0.6, 1.0),
        LongDataType)
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
  }

  it should "create a new revision by changing Transformer types between hashing and quantiles" in withSpark {
    spark =>
      // a: hashing -> quantiles, b: quantiles -> hashing
      val revision = Revision.firstRevision(
        QTableID("test"),
        1000,
        Vector(
          HashTransformer("a", LongDataType),
          CDFNumericQuantilesTransformer("b", DoubleDataType)),
        Vector(
          HashTransformation(),
          CDFNumericQuantilesTransformation(Vector(0d, 0.5, 1.0), DoubleDataType)))
      val options =
        QbeastOptions(
          Map(
            COLUMNS_TO_INDEX -> "a:quantiles,b:hashing",
            CUBE_SIZE -> "1000",
            COLUMN_STATS -> """{"a_quantiles":[0.0, 0.3, 0.6, 1.0]}"""))
      val newRevision =
        computeRevisionChanges(revision, options, createData(spark))._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        CDFNumericQuantilesTransformer("a", LongDataType),
        HashTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head shouldBe CDFNumericQuantilesTransformation(
        Vector(0.0, 0.3, 0.6, 1.0),
        LongDataType)
      newRevision.transformations.last shouldBe a[HashTransformation]
  }

  it should "throw an exception when using a quantile transformation without columnStats" in withSpark {
    spark =>
      val revision = Revision.firstRevision(
        QTableID("test"),
        1000,
        Vector(LinearTransformer("a", LongDataType), HashTransformer("b", DoubleDataType)))
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a:quantiles,b", CUBE_SIZE -> "1000"))
      val thrown = intercept[AnalysisException] {
        computeRevisionChanges(revision, options, createData(spark))
      }
      val smg =
        "Empty transformation for column a. " +
          "The following must be provided to use QuantileTransformers: a_quantiles."

      thrown.getMessage shouldBe smg
  }

  it should "not create a new revision when appending to an existing quantile transformation without columnStats" in
    withSpark { spark =>
      val revision = Revision.firstRevision(
        QTableID("test"),
        1000,
        Vector(
          CDFNumericQuantilesTransformer("a", LongDataType),
          HashTransformer("b", DoubleDataType)),
        Vector(
          CDFNumericQuantilesTransformation(Vector(0d, 0.5, 1.0), LongDataType),
          HashTransformation()))
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      computeRevisionChanges(revision, options, createData(spark))._1 shouldBe None
    }

  it should "throw an exception when the provided columnStats is not a valid JSON string" in
    withSpark { spark =>
      val revision = stagingRevision(QTableID("test"), 1000, Seq("a", "b"))
      val options = QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a:quantiles,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> """{"a_quantiles": [0.0, 0.3, 0.6, 1.0}"""))

      val thrown = intercept[AnalysisException] {
        computeRevisionChanges(revision, options, createData(spark))
      }

      thrown.getMessage shouldBe
        """The columnStats provided is not a valid JSON: {"a_quantiles": [0.0, 0.3, 0.6, 1.0}"""
    }

  // cubeSize changes
  it should "detect cubeSize changes" in withSpark { spark =>
    val data = createData(spark)
    val options = QbeastOptions(Map("columnsToIndex" -> "a,b", "cubeSize" -> "1000"))
    val startingRevision = SparkRevisionFactory
      .createNewRevision(QTableID("test"), data.schema, options)
    val revision_1 =
      computeRevisionChanges(startingRevision, options, data)._1.get.createNewRevision
    val revision_2 =
      computeRevisionChanges(
        revision_1,
        options.copy(cubeSize = 10000),
        data)._1.get.createNewRevision

    revision_2.desiredCubeSize shouldBe 10000
  }

  it should "createNewRevision with column stats for all numeric types" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val data = spark
      .range(5)
      .map(i => TestAll(s"$i", i.toDouble, i.floatValue(), i.toInt, i.toLong, BigDecimal(i)))
      .toDF()
    val emptyRevision = SparkRevisionFactory.createNewRevision(
      qid,
      data.schema,
      QbeastOptions(
        Map(COLUMNS_TO_INDEX -> "float_value,double_value,int_value,long_value,decimal_value")))

    // Compute The Revision Changes given a set of Column Stats
    val (revisionChanges, _) =
      computeRevisionChanges(
        emptyRevision, // empty revision
        QbeastOptions(
          Map(
            QbeastOptions.COLUMNS_TO_INDEX -> "float_value,double_value,int_value,long_value,decimal_value",
            QbeastOptions.COLUMN_STATS ->
              """{"float_value_min":0.0, "float_value_max":10.0,
                | "double_value_min":0.0, "double_value_max":10.0,
                | "int_value_min":0, "int_value_max":10,
                | "long_value_min":0, "long_value_max":10,
                | "decimal_value_min":0.0, "decimal_value_max":10.0}""".stripMargin)),
        data)

    // the revisionChanges should be defined
    revisionChanges shouldBe defined

    val revision = revisionChanges.get.createNewRevision
    revision.tableID shouldBe qid
    // the reason while it's 1 is because columnStats are provided here
    revision.revisionID shouldBe 1
    revision.columnTransformers shouldBe Vector(
      LinearTransformer("float_value", FloatDataType),
      LinearTransformer("double_value", DoubleDataType),
      LinearTransformer("int_value", IntegerDataType),
      LinearTransformer("long_value", LongDataType),
      LinearTransformer("decimal_value", DecimalDataType))

    val transformations = revision.transformations
    transformations should not be null
    transformations.head should matchPattern {
      case LinearTransformation(0.0f, 10.0f, _, FloatDataType) =>
    }
    transformations(1) should matchPattern {
      case LinearTransformation(0.0, 10.0, _, DoubleDataType) =>
    }
    transformations(2) should matchPattern {
      case LinearTransformation(0, 10, _, IntegerDataType) =>
    }
    transformations(3) should matchPattern {
      case LinearTransformation(0L, 10L, _, LongDataType) =>
    }
    transformations(4) should matchPattern {
      case LinearTransformation(0.0, 10.0, _, DecimalDataType) =>
    }

  })

}
