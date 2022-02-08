package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model.{
  CubeNormalizedWeight,
  DoubleDataType,
  FloatDataType,
  IndexStatus,
  IntegerDataType,
  QTableID,
  Revision
}
import io.qbeast.core.transform.{HashTransformation, LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  calculateRevisionChanges,
  estimateCubeWeights,
  estimatePartitionCubeWeights,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper

class DoublePassOTreeDataAnalyzerTest extends QbeastIntegrationTestSpec {

  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
    import spark.implicits._

    0.to(size)
      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))
      .toDF()
      .as[T3]

  }

  private val randomNullString = "10"

  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {

    columnsSchema
      .map(field => {
        field.dataType match {
          // for easier patter matching we would use HashTransformation("null")
          case org.apache.spark.sql.types.StringType =>
            Transformer(
              "hashing",
              field.name,
              randomNullString,
              SparkToQTypesUtils.convertDataTypes(field.dataType))
          case _ =>
            Transformer(
              "linear",
              field.name,
              randomNullString,
              SparkToQTypesUtils.convertDataTypes(field.dataType))
        }
      })
      .toIndexedSeq
  }

  it should "calculateRevisionChanges correctly on single column" in withSpark { spark =>
    val dataFrame = createDF(10000, spark).toDF()
    val columnsToIndex = Seq("c")
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)
    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    revision.revisionID shouldBe 1L
    revision.columnTransformers.size shouldBe 1
    revision.transformations.size shouldBe 1

    val columnTransformer = revision.columnTransformers.head
    columnTransformer.columnName shouldBe "c"
    columnTransformer.spec shouldBe s"c:hashing"

    val transformation = revision.transformations.head
    transformation mustBe a[HashTransformation]

  }

  it should "calculateRevisionChanges correctly on hashing types" in withSpark { spark =>
    import spark.implicits._

    val dataFrame = 0
      .to(10000)
      .map(i => TestStrings(s"$i", s"${i * i}", s"${i * 2}"))
      .toDF()
      .as[TestStrings]

    val columnsToIndex = Seq("a", "b", "c")
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)
    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    revision.columnTransformers.length shouldBe columnsToIndex.length
    revision.transformations.length shouldBe columnsToIndex.length
    revision.transformations shouldBe Vector.fill(columnsToIndex.length)(
      HashTransformation(randomNullString))
  }

  it should "calculateRevisionChanges correctly on different types" in withSpark { spark =>
    val dataFrame = createDF(10000, spark).toDF()
    val columnsToIndex = dataFrame.columns
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)
    val revisionChanges =
      calculateRevisionChanges(dataFrameStats, emptyRevision)
    revisionChanges shouldBe defined
    revisionChanges.get.supersededRevision shouldBe emptyRevision
    revisionChanges.get.transformationsChanges.count(_.isDefined) shouldBe columnsToIndex.length
    revisionChanges.get.desiredCubeSizeChange shouldBe None
    revisionChanges.get.columnTransformersChanges shouldBe Vector.empty

    val revision = revisionChanges.get.createNewRevision

    revision.revisionID shouldBe 1L
    revision.columnTransformers.size shouldBe columnsToIndex.length
    revision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
    revision.transformations.size shouldBe columnsToIndex.length

    revision.transformations shouldBe Vector(
      LinearTransformation(0, 10000, randomNullString.toInt, IntegerDataType),
      LinearTransformation(0.0, 10000.0, randomNullString.toDouble, DoubleDataType),
      HashTransformation(randomNullString),
      LinearTransformation(0.0.toFloat, 10000.0.toFloat, randomNullString.toFloat, FloatDataType))

  }

  it should "calculateRevisionChanges when the space gets bigger" in withSpark { spark =>
    import spark.implicits._

    val data = createDF(10000, spark)
    val columnsToIndex = data.columns
    val columnsSchema = data.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    val spaceMultiplier = 2
    val newRevisionDataFrame =
      data.map(t3 =>
        t3.copy(
          a = t3.a * spaceMultiplier,
          b = t3.b * spaceMultiplier,
          d = t3.d * spaceMultiplier))

    val newDataFrameStats = getDataFrameStats(newRevisionDataFrame.toDF(), columnTransformers)
    val revisionChanges =
      calculateRevisionChanges(newDataFrameStats, revision)
    val newRevision = revisionChanges.get.createNewRevision

    newRevision.revisionID shouldBe 2L
    newRevision.columnTransformers.size shouldBe columnsToIndex.length
    newRevision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
    newRevision.transformations.size shouldBe columnsToIndex.length

    newRevision.transformations shouldBe Vector(
      LinearTransformation(0, 10000 * spaceMultiplier, randomNullString.toInt, IntegerDataType),
      LinearTransformation(
        0.0,
        10000.0 * spaceMultiplier,
        randomNullString.toDouble,
        DoubleDataType),
      HashTransformation(randomNullString),
      LinearTransformation(
        0.0.toFloat,
        (10000.0 * spaceMultiplier).toFloat,
        randomNullString.toFloat,
        FloatDataType))

  }

  it should "estimatePartitionCubeWeights" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    val indexStatus = IndexStatus(revision, Set.empty)
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))
    val cubeNormalizedWeights =
      weightedDataFrame.transform(
        estimatePartitionCubeWeights(10000, revision, indexStatus, isReplication = false))

    val partitions = weightedDataFrame.rdd.getNumPartitions

    cubeNormalizedWeights
      .collect()
      .groupBy(_.cubeBytes)
      .foreach { case (_, weights: Array[CubeNormalizedWeight]) =>
        weights.foreach(w => w.normalizedWeight shouldBe >(0.0))
        weights.length shouldBe <=(partitions)
      }
  }

  it should "estimateCubeWeights" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    val indexStatus = IndexStatus(revision, Set.empty)
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))
    val cubeNormalizedWeights =
      weightedDataFrame.transform(
        estimatePartitionCubeWeights(10000, revision, indexStatus, isReplication = false))

    val cubeWeights = cubeNormalizedWeights.transform(estimateCubeWeights(revision))
    cubeWeights.columns.length shouldBe 2

    val cubeWeightsCollect = cubeWeights.collect()
    cubeWeightsCollect.map(_._1).distinct.length shouldBe cubeWeightsCollect.length
    cubeWeightsCollect.foreach { case (_, weight) =>
      weight shouldBe >(0.0)
    }

  }

}
