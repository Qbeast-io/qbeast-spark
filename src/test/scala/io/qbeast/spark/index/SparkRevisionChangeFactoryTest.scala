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

import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.COLUMN_STATS
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.Revision
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class SparkRevisionChangeFactoryTest extends QbeastIntegrationTestSpec {

  "SparkRevisionChangeFactory" should "create a new space with dataframe stats" in withSpark {
    _ =>
      // Data schema
      val dataSchema = StructType(
        Seq(
          StructField("a", LongType, nullable = true),
          StructField("b", LongType, nullable = true),
          StructField("c", LongType, nullable = true)))

      // dataFrameStatsRow
      val dataFrameStatsRow = new GenericRowWithSchema(
        Array(-100L, 100L, -200L, 200L),
        StructType(
          Seq(
            StructField("a_min", LongType, nullable = true),
            StructField("a_max", LongType, nullable = true),
            StructField("b_min", LongType, nullable = true),
            StructField("b_max", LongType, nullable = true))))
      val options = QbeastOptions(Map("columnsToIndex" -> "a,b", "cubeSize" -> "1000"))
      val firstRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), dataSchema, options)
      val newRevision =
        SparkRevisionChangeFactory
          .create(firstRevision, options, dataFrameStatsRow)
          .get
          .createNewRevision
      newRevision.columnTransformers shouldBe Seq(
        LinearTransformer("a", LongDataType),
        LinearTransformer("b", LongDataType)).toIndexedSeq
      newRevision.transformations.size shouldBe 2
      newRevision.transformations.head should matchPattern {
        case LinearTransformation(-100L, 100L, _, LongDataType) =>
      }
      newRevision.transformations.last should matchPattern {
        case LinearTransformation(-200L, 200L, _, LongDataType) =>
      }
  }

  it should "create a new space with columnStats smaller than dataframe stats" in withSpark { _ =>
    // Data schema
    val dataSchema = StructType(
      Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true),
        StructField("c", LongType, nullable = true)))

    // dataFrameStatsRow
    val dataFrameStatsRow = new GenericRowWithSchema(
      Array(-100L, 100L, -200L, 200L),
      StructType(
        Seq(
          StructField("a_min", LongType, nullable = true),
          StructField("a_max", LongType, nullable = true),
          StructField("b_min", LongType, nullable = true),
          StructField("b_max", LongType, nullable = true))))
    val options =
      QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-10,"a_max":10, "b_min":-20, "b_max":20}"""))
    val firstRevision =
      SparkRevisionFactory.createNewRevision(QTableID("test"), dataSchema, options)

    val newRevision =
      SparkRevisionChangeFactory
        .create(firstRevision, options, dataFrameStatsRow)
        .get
        .createNewRevision
    newRevision.columnTransformers shouldBe Seq(
      LinearTransformer("a", LongDataType),
      LinearTransformer("b", LongDataType)).toIndexedSeq
    newRevision.transformations.size shouldBe 2
    newRevision.transformations.head should matchPattern {
      case LinearTransformation(-100L, 100L, _, LongDataType) =>
    }
    newRevision.transformations.last should matchPattern {
      case LinearTransformation(-200L, 200L, _, LongDataType) =>
    }
  }

  it should "create a new space with columnStats larger than dataframe stats" in withSpark { _ =>
    // Data schema
    val dataSchema = StructType(
      Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true),
        StructField("c", LongType, nullable = true)))

    // dataFrameStatsRow
    val dataFrameStatsRow = new GenericRowWithSchema(
      Array(-100L, 100L, -200L, 200L),
      StructType(
        Seq(
          StructField("a_min", LongType, nullable = true),
          StructField("a_max", LongType, nullable = true),
          StructField("b_min", LongType, nullable = true),
          StructField("b_max", LongType, nullable = true))))
    val options =
      QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-1000,"a_max":1000, "b_min":-2000, "b_max":2000}"""))
    val firstRevision =
      SparkRevisionFactory.createNewRevision(QTableID("test"), dataSchema, options)

    val newRevision =
      SparkRevisionChangeFactory
        .create(firstRevision, options, dataFrameStatsRow)
        .get
        .createNewRevision
    newRevision.columnTransformers shouldBe Seq(
      LinearTransformer("a", LongDataType),
      LinearTransformer("b", LongDataType)).toIndexedSeq
    newRevision.transformations.size shouldBe 2
    newRevision.transformations.head should matchPattern {
      case LinearTransformation(-1000L, 1000L, _, LongDataType) =>
    }
    newRevision.transformations.last should matchPattern {
      case LinearTransformation(-2000L, 2000L, _, LongDataType) =>
    }
  }

  it should "not create a new space when the current space is not superseded" in withSpark { _ =>
    // dataFrameStatsRow
    val dataFrameStatsRow = new GenericRowWithSchema(
      Array(-100L, 100L, -200L, 200L),
      StructType(
        Seq(
          StructField("a_min", LongType, nullable = true),
          StructField("a_max", LongType, nullable = true),
          StructField("b_min", LongType, nullable = true),
          StructField("b_max", LongType, nullable = true))))
    val options =
      QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "a,b",
          CUBE_SIZE -> "1000",
          COLUMN_STATS -> s"""{"a_min":-1000,"a_max":1000, "b_min":-2000, "b_max":2000}"""))
    val revision =
      Revision.firstRevision(
        QTableID("test"),
        options.cubeSize,
        Vector(LinearTransformer("a", LongDataType), LinearTransformer("b", LongDataType)),
        Vector(
          LinearTransformation(-10000L, 10000L, "a", LongDataType),
          LinearTransformation(-20000L, 20000L, "b", LongDataType)))
    val spaceChange = SparkRevisionChangeFactory.create(revision, options, dataFrameStatsRow)
    spaceChange shouldBe None
  }


  //  it should "work correctly with columnStats smaller than input dataframe stats" in withSpark {
  //    spark =>
  //      // case class T3(a: Long, b: Double, c: String, d: Float)
  //      // 0 to 9999
  //      val dataFrame = createDF(10000, spark).toDF()
  //      val options = QbeastOptions(
  //        Map(
  //          "columnsToIndex" -> "a,b",
  //          "cubeSize" -> "1000",
  //          "columnStats" -> s"""{"a_min":-10,"a_max":10, "b_min":-10.0, "b_max":10.0}"""))
  //      val revision =
  //        SparkRevisionFactory.createNewRevision(QTableID("test"), dataFrame.schema, options)
  //      val row = getDataFrameStats(dataFrame, revision.columnTransformers)
  //      val changes = computeTransformationChanges(row, revision, options)
  //      changes.size shouldBe 2
  //      changes.head should matchPattern {
  //        case Some(LinearTransformation(0L, 9999L, _, LongDataType)) =>
  //      }
  //      changes.last should matchPattern {
  //        case Some(LinearTransformation(0.0, 9999.0, _, DoubleDataType)) =>
  //      }
  //  }
  //
  //  it should "computeRevisionChanges correctly on single column" in withSpark { spark =>
  //    val dataFrame = createDF(10000, spark).toDF()
  //    val options = QbeastOptions(Map("columnsToIndex" -> "c", "cubeSize" -> "1000"))
  //    val startingRevision =
  //      SparkRevisionFactory.createNewRevision(QTableID("test"), dataFrame.schema, options)
  //    val dataFrameStats = getDataFrameStats(dataFrame, startingRevision.columnTransformers)
  //    val revision =
  //      computeRevisionChanges(dataFrameStats, startingRevision, options).get.createNewRevision
  //
  //    revision.revisionID shouldBe 1L
  //    revision.columnTransformers.size shouldBe 1
  //    revision.transformations.size shouldBe 1
  //
  //    val columnTransformer = revision.columnTransformers.head
  //    columnTransformer.columnName shouldBe "c"
  //    columnTransformer.spec shouldBe "c:hashing"
  //
  //    val transformation = revision.transformations.head
  //    transformation mustBe a[HashTransformation]
  //  }
  //
  //  "computeRevisionChanges" should "work correctly with hashing types" in withSpark { spark =>
  //    import spark.implicits._
  //    val dataFrame = spark
  //      .range(10000)
  //      .map(i => TestStrings(s"$i", s"${i * i}", s"${i * 2}"))
  //    val options = QbeastOptions(Map("columnsToIndex" -> "a,b,c", "cubeSize" -> "1000"))
  //    val startingRevision =
  //      SparkRevisionFactory.createNewRevision(QTableID("test"), dataFrame.schema, options)
  //    val dataFrameStats = getDataFrameStats(dataFrame.toDF(), startingRevision.columnTransformers)
  //    val revision =
  //      computeRevisionChanges(dataFrameStats, startingRevision, options).get.createNewRevision
  //
  //    revision.columnTransformers.length shouldBe options.columnsToIndex.length
  //    revision.transformations.length shouldBe options.columnsToIndex.length
  //    revision.transformations.foreach(t => t shouldBe a[HashTransformation])
  //  }
  //
  //  it should "work correctly with different types" in withSpark { spark =>
  //    val dataFrame = createDF(10001, spark).toDF()
  //    val columnsToIndex = dataFrame.columns
  //    val options =
  //      QbeastOptions(Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> "1000"))
  //    val startingRevision =
  //      SparkRevisionFactory.createNewRevision(QTableID("test"), dataFrame.schema, options)
  //    val dataFrameStats = getDataFrameStats(dataFrame, startingRevision.columnTransformers)
  //
  //    val revisionChanges = computeRevisionChanges(dataFrameStats, startingRevision, options)
  //    revisionChanges shouldBe defined
  //    revisionChanges.get.supersededRevision shouldBe startingRevision
  //    revisionChanges.get.transformationsChanges.count(_.isDefined) shouldBe columnsToIndex.length
  //    revisionChanges.get.desiredCubeSizeChange shouldBe None
  //    revisionChanges.get.columnTransformersChanges shouldBe Vector.empty
  //
  //    val revision = revisionChanges.get.createNewRevision
  //    revision.revisionID shouldBe 1L
  //    revision.columnTransformers.size shouldBe columnsToIndex.length
  //    revision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
  //    revision.transformations.size shouldBe columnsToIndex.length
  //
  //    val zero = revision.transformations.head
  //    val one = revision.transformations(1)
  //    val two = revision.transformations(2)
  //    val three = revision.transformations(3)
  //
  //    zero should matchPattern { case LinearTransformation(0, 10000, _, LongDataType) => }
  //    one should matchPattern { case LinearTransformation(0.0, 10000.0, _, DoubleDataType) => }
  //    two should matchPattern { case HashTransformation(_) => }
  //    three should matchPattern { case LinearTransformation(0.0f, 10000.0f, _, FloatDataType) => }
  //
  //  }
  //
  //  it should "work correctly when the space gets bigger" in withSpark { spark =>
  //    import spark.implicits._
  //    val data = createDF(10001, spark)
  //    val columnsToIndex = data.columns
  //    val options =
  //      QbeastOptions(Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> "1000"))
  //    val startingRevision =
  //      SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
  //    val dataFrameStats = getDataFrameStats(data.toDF(), startingRevision.columnTransformers)
  //    val revision =
  //      computeRevisionChanges(dataFrameStats, startingRevision, options).get.createNewRevision
  //
  //    val spaceMultiplier = 2
  //    val newRevisionDataFrame =
  //      data.map(t3 =>
  //        t3.copy(
  //          a = t3.a * spaceMultiplier,
  //          b = t3.b * spaceMultiplier,
  //          d = t3.d * spaceMultiplier))
  //
  //    val newDataFrameStats =
  //      getDataFrameStats(newRevisionDataFrame.toDF(), revision.columnTransformers)
  //    val revisionChanges = computeRevisionChanges(newDataFrameStats, revision, options)
  //    val newRevision = revisionChanges.get.createNewRevision
  //
  //    newRevision.revisionID shouldBe 2L
  //    newRevision.columnTransformers.size shouldBe columnsToIndex.length
  //    newRevision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
  //    newRevision.transformations.size shouldBe columnsToIndex.length
  //
  //    val zero = newRevision.transformations.head
  //    val one = newRevision.transformations(1)
  //    val two = newRevision.transformations(2)
  //    val three = newRevision.transformations(3)
  //
  //    zero should matchPattern { case LinearTransformation(0, 20000, _, LongDataType) => }
  //    one should matchPattern { case LinearTransformation(0.0, 20000.0, _, DoubleDataType) => }
  //    two should matchPattern { case HashTransformation(_) => }
  //    three should matchPattern { case LinearTransformation(0.0f, 20000.0f, _, FloatDataType) =>
  //    }
  //
  //  }

}
