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
import io.qbeast.core.model.BroadcastedTableChanges
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T1
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.SparkException

import scala.util.Random

class SparkPointWeightIndexerTest extends QbeastIntegrationTestSpec {

  behavior of "SparkPointWeightIndexerTest"

  it should "buildIndex should fail with empty transformation" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val df = spark.range(11).map(a => T1(a, a.toString, a.toDouble))
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      QbeastOptions(Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c")))

    val indexStatus = IndexStatus(rev)
    val tableChanges = BroadcastedTableChanges(None, indexStatus, Map.empty, Map.empty)
    val r = udf(() => {
      Random.nextInt
    })
    val df2 = df.withColumn(QbeastColumns.weightColumnName, r())
    val spwi = new SparkPointWeightIndexer(tableChanges, false)

    the[SparkException] thrownBy {
      df2.transform(spwi.buildIndex).select(col(QbeastColumns.cubeColumnName)).distinct.first()
    }

  })

  it should "buildIndex works with different indexed columns" in withSpark(spark => {
    import spark.implicits._
    val qid = QTableID("t")
    val df = spark.range(11).map(a => T1(a, a.toString, a.toDouble))
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      QbeastOptions(Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c")))
    val indexStatus = IndexStatus(rev)

    val revisionChange =
      RevisionChange(
        0,
        supersededRevision = rev,
        desiredCubeSizeChange = None,
        columnTransformersChanges = Nil,
        transformationsChanges = Vector(
          Some(LinearTransformation(0, 10, IntegerDataType)),
          Some(HashTransformation()),
          Some(LinearTransformation(0.0, 10.0, DoubleDataType))))
    val tc = BroadcastedTableChanges(Some(revisionChange), indexStatus, Map.empty, Map.empty)

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
    val df = spark.range(11).map(a => T1(a, a.toString, a.toDouble))
    val rev = SparkRevisionFactory.createNewRevision(
      qid,
      df.schema,
      QbeastOptions(Map(QbeastOptions.COLUMNS_TO_INDEX -> "a:hashing,b:hashing,c:hashing")))
    val indexStatus = IndexStatus(rev)

    val revisionChange =
      RevisionChange(
        0,
        supersededRevision = rev,
        desiredCubeSizeChange = None,
        columnTransformersChanges = Nil,
        transformationsChanges = Vector(
          Some(HashTransformation()),
          Some(HashTransformation()),
          Some(HashTransformation())))
    val tc = BroadcastedTableChanges(Some(revisionChange), indexStatus, Map.empty, Map.empty)

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
