package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model._
import io.qbeast.core.transform.{HashTransformation, LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  calculateRevisionChanges,
  computeGlobalDomain,
  computeLocalTrees,
  estimateCubeWeights,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper

class DoublePassOTreeDataAnalyzerTest extends QbeastIntegrationTestSpec {

  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
    import spark.implicits._

    0.to(size)
      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))
      .toDF()
      .as[T3]

  }

  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {
    columnsSchema
      .map(field => Transformer(field.name, SparkToQTypesUtils.convertDataTypes(field.dataType)))
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
    revision.transformations.foreach(t => t shouldBe a[HashTransformation])
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

    val zero = revision.transformations.head
    val one = revision.transformations(1)
    val two = revision.transformations(2)
    val three = revision.transformations(3)

    zero should matchPattern { case LinearTransformation(0, 10000, _, IntegerDataType) => }
    one should matchPattern { case LinearTransformation(0.0, 10000.0, _, DoubleDataType) => }
    two should matchPattern { case HashTransformation(_) => }
    three should matchPattern { case LinearTransformation(0.0f, 10000.0f, _, FloatDataType) => }

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

    val zero = newRevision.transformations.head
    val one = newRevision.transformations(1)
    val two = newRevision.transformations(2)
    val three = newRevision.transformations(3)

    zero should matchPattern { case LinearTransformation(0, 20000, _, IntegerDataType) => }
    one should matchPattern { case LinearTransformation(0.0, 20000.0, _, DoubleDataType) => }
    two should matchPattern { case HashTransformation(_) => }
    three should matchPattern { case LinearTransformation(0.0f, 20000.0f, _, FloatDataType) =>
    }

  }

  it should "compute local trees correctly" in withSpark { spark =>
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

    val localTrees =
      weightedDataFrame
        .transform(computeLocalTrees(10000, revision, indexStatus, isReplication = false))
        .collect()

    var dataSize = 0d
    val root = revision.createCubeIdRoot()

    localTrees.foreach(t => {
      // Update data size
      dataSize += t(root).treeSize

      // Root node must be present in all localTrees
      t.contains(root) shouldBe true

      t.values.foreach(info => {
        info.parentWeight shouldBe >=(0d)
        // No node should have empty payload
        info.treeSize shouldBe >(0d)
      })
    })

    // The root nodes should have a treeSizes equal to the size of their corresponding partitions
    dataSize shouldBe dataFrameStats.getAs[Long]("count")
  }

  it should "compute global tree size and cube domain correctly" in withSpark { spark =>
    import spark.implicits._

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
    val localTrees =
      weightedDataFrame
        .transform(computeLocalTrees(10000, revision, indexStatus, isReplication = false))

    val globalDomain =
      computeGlobalDomain(
        localTrees,
        isReplication = false,
        columnTransformers.size,
        revision.desiredCubeSize).sortBy(_._1)

    val globalDomainMap = globalDomain.toMap

    // Should have the correct number of cubes
    globalDomain.size shouldBe globalDomainMap.size
    globalDomainMap.size shouldBe localTrees.flatMap(_.keys).distinct().count()

    // Root should have the correct domain
    val rootDomain = globalDomainMap(revision.createCubeIdRoot())
    rootDomain shouldBe dataFrameStats.getAs[Long]("count")

    // treeSize and domain are monotonically decreasing along branches
    globalDomain.foreach { case (cube, domain) =>
      // monotonically decreasing tree size and domain
      cube.children
        .filter(globalDomain.contains)
        .foreach(child => {
          globalDomainMap(child) shouldBe <=(domain)
        })
    }
  }

  it should "estimate global NormalizedWeight's correctly" in withSpark { spark =>
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

    val localTrees =
      weightedDataFrame
        .transform(computeLocalTrees(10000, revision, indexStatus, isReplication = false))

    val globalDomain =
      computeGlobalDomain(
        localTrees,
        isReplication = false,
        columnTransformers.size,
        revision.desiredCubeSize)

    val cubeWeights =
      estimateCubeWeights(globalDomain, indexStatus, isReplication = false)

    // Cubes with a weight lager than 1d should not have children
    val leafCubesByWeight = cubeWeights.filter(cw => cw._2 >= 1d).keys
    leafCubesByWeight.exists(cube => cube.children.exists(cubeWeights.contains)) shouldBe false

    // Test cubeWeights is monotonically increasing in all branches
    cubeWeights.toSeq
      .sortBy(_._1)
      .foreach { case (cube, weight) =>
        cube.children
          .filter(cubeWeights.contains)
          .foreach(child => weight < cubeWeights(child))
      }
  }
}
