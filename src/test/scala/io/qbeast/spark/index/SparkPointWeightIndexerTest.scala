/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses.T1
import io.qbeast.model._
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.transform.{HashTransformation, LinearTransformation}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{col, udf}

import scala.util.Random

class SparkPointWeightIndexerTest extends QbeastIntegrationTestSpec {

  behavior of "SparkPointWeightIndexerTest"

  it should "addState" in withSpark(spark => {

    import spark.implicits._
    val addState = SparkPointWeightIndexer.addState(
      3,
      Set(CubeId.root(3).firstChild.firstChild),
      Set(CubeId.root(3), CubeId.root(3).firstChild))

    val cubes = (Seq(CubeId.root(3), CubeId.root(3).firstChild.firstChild) ++
      CubeId.root(3).children.take(8))
      .map(_.bytes)
      .toDF(QbeastColumns.cubeColumnName)

    val indexed = cubes
      .transform(addState)
      .collect()
      .map(b => (CubeId(3, b.getAs[Array[Byte]](0)), b.getString(1)))

    indexed.find(_._1.isRoot) shouldBe Some((CubeId.root(3), "REPLICATED"))
    val sec = CubeId.root(3).firstChild
    indexed.find(_._1 == sec) shouldBe Some((sec, "REPLICATED"))

    val gen3 = CubeId.root(3).firstChild.firstChild
    indexed.find(_._1 == gen3) shouldBe Some((gen3, "ANNOUNCED"))

    indexed.count(_._2 == "FLOODED") shouldBe 7

  })

  it should "buildIndex should fail with empty transformation" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val df = 0.to(10).map(a => T1(a, a.toString, a.toDouble)).toDF()
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c"))

    val indexStatus = IndexStatus(rev)
    val isc = IndexStatusChange(indexStatus, Map.empty)
    val r = udf(() => {
      Random.nextInt
    })
    val df2 = df.withColumn(QbeastColumns.weightColumnName, r())
    val spwi = new SparkPointWeightIndexer(TableChanges(None, isc), false)

    the[SparkException] thrownBy {
      df2.transform(spwi.buildIndex).select(col(QbeastColumns.cubeColumnName)).distinct.first()
    }

  })

  it should "buildIndex works with different indexed columns" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val df = 0.to(10).map(a => T1(a, a.toString, a.toDouble)).toDF()
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c"))
    val indexStatus = IndexStatus(rev)

    val isc = IndexStatusChange(indexStatus, Map.empty)

    val tc = TableChanges(
      Some(
        RevisionChange(
          0,
          supersededRevision = rev,
          desiredCubeSizeChange = None,
          columnTransformersChanges = Nil,
          transformationsChanges = Vector(
            Some(LinearTransformation(0, 10, IntegerDataType)),
            Some(HashTransformation()),
            Some(LinearTransformation(0.0, 10.0, DoubleDataType))))),
      isc)

    val r = udf(() => {
      Random.nextInt
    })
    val df2 = df.withColumn(QbeastColumns.weightColumnName, r())
    val spwi = new SparkPointWeightIndexer(tc, false)
    val root =
      df2.transform(spwi.buildIndex).select(col(QbeastColumns.cubeColumnName)).distinct.first()

    CubeId(3, root.getAs[Array[Byte]](0)) shouldBe CubeId.root(3)

  })

  it should "buildIndex when we hash all columns" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val df = 0.to(10).map(a => T1(a, a.toString, a.toDouble)).toDF()
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      Map(QbeastOptions.COLUMNS_TO_INDEX -> "a:hashing,b:hashing,c:hashing"))
    val indexStatus = IndexStatus(rev)

    val isc = IndexStatusChange(indexStatus, Map.empty)

    val tc = TableChanges(
      Some(
        RevisionChange(
          0,
          supersededRevision = rev,
          desiredCubeSizeChange = None,
          columnTransformersChanges = Nil,
          transformationsChanges = Vector(
            Some(HashTransformation()),
            Some(HashTransformation()),
            Some(HashTransformation())))),
      isc)

    val r = udf(() => {
      Random.nextInt
    })
    val df2 = df.withColumn(QbeastColumns.weightColumnName, r())
    val spwi = new SparkPointWeightIndexer(tc, false)
    val root =
      df2.transform(spwi.buildIndex).select(col(QbeastColumns.cubeColumnName)).distinct.first()

    CubeId(3, root.getAs[Array[Byte]](0)) shouldBe CubeId.root(3)

  })

}
