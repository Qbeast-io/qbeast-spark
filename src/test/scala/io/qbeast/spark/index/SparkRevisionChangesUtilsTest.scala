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

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.COLUMN_STATS
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.transform.CDFNumericQuantilesTransformation
import io.qbeast.core.transform.CDFNumericQuantilesTransformer
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T3
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

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

  "SparkRevisionChangesUtils" should "create a new space with dataframe stats alone" in
    withSpark { spark =>
      val data = createData(spark)
      val options = QbeastOptions(Map("columnsToIndex" -> "a,b", "cubeSize" -> "1000"))
      val revision = SparkRevisionFactory
        .createNewRevision(QTableID("test"), data.schema, options)
      val newRevision = computeRevisionChanges(revision, options, data)._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
    }

  it should "create a new space with columnStats smaller than dataframe stats" in withSpark {
    spark =>
      val data = createData(spark)
      val options = QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-10,"a_max":10, "b_min":-20.0, "b_max":20.0}"""))
      val revision = SparkRevisionFactory
        .createNewRevision(QTableID("test"), data.schema, options)
      val newRevision = computeRevisionChanges(revision, options, data)._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
      }
  }

  it should "create a new space with columnStats larger than dataframe stats" in withSpark {
    spark =>
      val data = createData(spark)
      val options = QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}"""))
      val revision = SparkRevisionFactory
        .createNewRevision(QTableID("test"), data.schema, options)
      val newRevision = computeRevisionChanges(revision, options, data)._1.get.createNewRevision

      newRevision.columnTransformers shouldBe Vector(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", DoubleDataType))
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head should matchPattern {
        case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
      }
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
      }
  }

  it should "not create a new space WITHOUT columnStats when the current space is not superseded" in
    withSpark { spark =>
      val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
      val revision = Revision.firstRevision(
        QTableID("test"),
        options.cubeSize,
        Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
        Vector(
          LinearTransformation(-10000L, 10000L, "a", LongDataType),
          LinearTransformation(-20000.0, 20000.0, "b", DoubleDataType)))
      val data = createData(spark)
      val spaceChange = computeRevisionChanges(revision, options, data)._1
      spaceChange shouldBe None
    }

  it should "not create a new space WITH columnStats when the current space is not superseded" in
    withSpark { spark =>
      val options = QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}"""))
      val revision = Revision.firstRevision(
        QTableID("test"),
        options.cubeSize,
        Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", DoubleDataType)),
        Vector(
          LinearTransformation(-10000L, 10000L, "a", LongDataType),
          LinearTransformation(-20000.0, 20000.0, "b", DoubleDataType)))
      val data = createData(spark)
      val spaceChange = computeRevisionChanges(revision, options, data)._1

      spaceChange shouldBe None
    }

  it should "create a new space with staging Revision and NO columnStats" in withSpark { spark =>
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
    val stagingRev = stagingRevision(QTableID("test"), options.cubeSize, options.columnsToIndex)
    val newRevision =
      computeRevisionChanges(stagingRev, options, createData(spark))._1.get.createNewRevision

    newRevision.columnTransformers shouldBe Vector(
      LinearTransformer("a", LongDataType),
      LinearTransformer("b", DoubleDataType))
    newRevision.transformations.size shouldBe 2
    newRevision.transformations.head should matchPattern {
      case LinearTransformation(-100L, 100L, _, LongDataType) =>
    }
    newRevision.transformations.last should matchPattern {
      case LinearTransformation(-200.0, 200.0, _, DoubleDataType) =>
    }
  }

  it should "create a new space with staging Revision AND columnStats" in withSpark { spark =>
    val options = QbeastOptions(
      Map(
        COLUMNS_TO_INDEX -> "a,b",
        CUBE_SIZE -> "1000",
        COLUMN_STATS ->
          s"""{"a_min":-1000,"a_max":1000, "b_min":-2000.0, "b_max":2000.0}"""))
    val stagingRev = stagingRevision(QTableID("test"), options.cubeSize, options.columnsToIndex)
    val newRevision =
      computeRevisionChanges(stagingRev, options, createData(spark))._1.get.createNewRevision

    newRevision.columnTransformers shouldBe Vector(
      LinearTransformer("a", LongDataType),
      LinearTransformer("b", DoubleDataType))
    newRevision.transformations.size shouldBe 2
    newRevision.transformations.head should matchPattern {
      case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
    }
    newRevision.transformations.last should matchPattern {
      case LinearTransformation(-2000.0, 2000.0, _, DoubleDataType) =>
    }
  }

  it should "throw an exception when trying to change indexing columns" in withSpark { spark =>
    val data = createData(spark)
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,b", CUBE_SIZE -> "1000"))
    val revision = SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
    val newOptions = QbeastOptions(Map(COLUMNS_TO_INDEX -> "a,c", CUBE_SIZE -> "1000"))
    an[AnalysisException] shouldBe thrownBy(computeRevisionChanges(revision, newOptions, data))
  }

  it should "allow for changing Transformer types between hashing to linear" in withSpark {
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

  it should "allow for changing Transformer types between linear and quantiles" in withSpark {
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

  it should "allow for changing Transformer types between hashing and quantiles" in withSpark {
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
      val smg = s"Empty transformation for column a. " +
        s"The following must be provided to use QuantileTransformers: a_quantiles."

      thrown.getMessage shouldBe smg
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

  it should "change transformer type only when specified" in withSpark { spark =>
    val schema = createData(spark).schema
    val noChanges = computeTransformerChanges(
      Vector(CDFNumericQuantilesTransformer("b", DoubleDataType)),
      QbeastOptions(Seq("b"), 1000, "some_format"),
      schema).flatten

    noChanges.isEmpty shouldBe true

    val withChanges = computeTransformerChanges(
      Vector(CDFNumericQuantilesTransformer("b", DoubleDataType)),
      QbeastOptions(Seq("b:hashing"), 1000, "some_format"),
      schema).flatten
    withChanges shouldBe Vector(HashTransformer("b", DoubleDataType))
  }

}
