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

import io.qbeast.core.model.QTableId
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class OTreeAlgorithmTest extends QbeastIntegrationTestSpec {

  "addRandomWeight" should
    "be deterministic when a row have only nullable columns" in withQbeastAndSparkContext() {
      spark =>
        import spark.implicits._
        val df = spark
          .range(1000)
          .map(i => Client1(Some(i * i), Some(s"student-$i"), Some(i.intValue())))
          .toDF()
        checkRDD(df)

    }

  it should "be deterministic when a row have only strings" in
    withQbeastAndSparkContext() { spark =>
      import spark.implicits._
      val df = spark
        .range(1000)
        .map(i => ClientString((i * i).toString, s"student-$i", i.toString))
        .toDF()
      checkRDD(df)

    }

  it should "be deterministic when a row have only optional strings" in
    withQbeastAndSparkContext() { spark =>
      import spark.implicits._
      val df = spark
        .range(1000)
        .map(i =>
          ClientStringOption(Some((i * i).toString), Some(s"student-$i"), Some(i.toString)))
        .toDF()
      checkRDD(df)
    }

  it should "be deterministic with real data" in withQbeastAndSparkContext() { spark =>
    val inputPath = "src/test/resources/"
    val file1 = "ecommerce300k_2019_Nov.csv"
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputPath + file1)
      .distinct()
    checkRDD(df)
  }

  it should "be deterministic when a row has only nullable columns" +
    " and null values" in withQbeastAndSparkContext() { spark =>
      import spark.implicits._
      val df = spark
        .range(1000)
        .map {
          case i if i % 3 == 0 => Client1(Some(i * i), None, Some(i.intValue()))
          case i if i % 5 == 0 => Client1(Some(i * i), Some(s"student-$i"), None)
          case i if i % 7 == 0 => Client1(None, Some(s"student-$i"), Some(i.intValue()))
          case i if i % 11 == 0 => Client1(None, None, None)
          case i => Client1(Some(i * i), Some(s"student-$i"), Some(i.intValue()))
        }
        .toDF()

      checkRDD(df)

    }

  it should "be deterministic when a row have only NOT nullable columns" in
    withQbeastAndSparkContext() { spark =>
      import spark.implicits._
      val df =
        spark.range(1000).map(i => Client2(i * i, s"student-$i", i.intValue())).toDF()
      checkRDD(df)

    }

  def checkRDD(df: DataFrame): Unit =
    withQbeastAndSparkContext() { _ =>
      val rev = SparkRevisionFactory.createNewRevision(
        QTableId("test"),
        df.schema,
        QbeastOptions(Map("columnsToIndex" -> df.columns.mkString(","), "cubeSize" -> "10000")))

      val newDf = df.transform(DoublePassOTreeDataAnalyzer.addRandomWeight(rev))
      /* With less than 10k rows the probability of a collision is approximately 0.3%,
    show it should not happen  calculated with
    https://gist.github.com/benhoyt/b59c00fc47361b67bfdedc92e86b03eb#file-birthday_probability-py
       */

      val df2 = df.transform(DoublePassOTreeDataAnalyzer.addRandomWeight(rev))
      df2.agg(sum(col(weightColumnName))).first().get(0) shouldBe newDf
        .agg(sum(col(weightColumnName)))
        .first()
        .get(0)
    }

}
