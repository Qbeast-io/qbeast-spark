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
package io.qbeast.spark.delta.index

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester

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
    val tableId = new QTableID(directory)
    val qbeastSnapshot = new DeltaQbeastSnapshot(tableId)
    val lastRevision = qbeastSnapshot.loadLatestRevision
    val dfqbeast = spark.read.format("qbeast").load(directory)
    dfqbeast.createTempView("dfqbeast")

    lastRevision
  }

  def getMaxMinFromRevision(revision: Revision, spark: SparkSession): Seq[Seq[Any]] = {
    for (t <- revision.columnTransformers)
      yield spark
        .table("dfqbeast")
        .select(max(t.columnName), min(t.columnName))
        .first()
        .toSeq
        .toVector
  }

  "LinearTransformer" should
    "get correct values with no nullable numbers" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        import spark.implicits._
        val df = spark
          .range(101)
          .map(i => Client3(i * i, s"student-$i", i.intValue(), i * 1000 + 123, i * 2567.3432143))
          .toDF()

        val cols = "age,val2,val3"
        val lastRevision = writeAndLoadDF(df, cols, tmpDir, spark)

        getMaxMinFromRevision(lastRevision, spark) shouldBe List(
          Vector(100, 0),
          Vector(100123, 123),
          Vector(256734.32143, 0.0))
    }

  it should "get correct values with nullable numbers" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      import spark.implicits._
      val df = spark
        .range(101)
        .map(i =>
          Client4(
            i * i,
            s"student-$i",
            Some(i.intValue()),
            Some(i * 1000 + 123),
            Some(i * 2567.3432143)))
        .toDF()

      val cols = "age,val2,val3"
      val lastRevision = writeAndLoadDF(df, cols, tmpDir, spark)

      getMaxMinFromRevision(lastRevision, spark) shouldBe List(
        Vector(100.0, 0.0),
        Vector(100123.0, 123.0),
        Vector(256734.32143, 0.0))
  }

}
