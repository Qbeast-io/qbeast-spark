/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.QTableID
import io.qbeast.spark.index.OTreeAlgorithmTest._
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object OTreeAlgorithmTest {
  case class Client1(id: Option[Long], name: Option[String], age: Option[Int])

  case class ClientString(id: String, name: String, age: String)

  case class ClientStringOption(id: Option[String], name: Option[String], age: Option[String])

  case class Client2(id: Long, name: String, age: Int)

  case class Client3(id: Long, name: String, age: Int, val2: Long, val3: Double)

  case class Client4(
      id: Long,
      name: String,
      age: Option[Int],
      val2: Option[Long],
      val3: Option[Double])

}

class OTreeAlgorithmTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {
  private val addRandomWeight = PrivateMethod[DataFrame]('addRandomWeight)

  "addRandomWeight" should
    "be deterministic when a row have only nullable columns" in withQbeastContext() {
      withSpark { spark =>
        val rdd = spark.sparkContext.parallelize(
          0.to(1000).map(i => Client1(Some(i * i), Some(s"student-$i"), Some(i))))
        val df = spark.createDataFrame(rdd)
        checkRDD(df)
      }
    }

  it should "be deterministic when a row have only strings" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd = spark.sparkContext.parallelize(
        0.to(1000).map(i => ClientString((i * i).toString, s"student-$i", i.toString)))
      val df = spark.createDataFrame(rdd)
      checkRDD(df)
    }
  }

  it should "be deterministic when a row have only optional strings" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd = spark.sparkContext.parallelize(
        0.to(1000)
          .map(i =>
            ClientStringOption(Some((i * i).toString), Some(s"student-$i"), Some(i.toString))))
      val df = spark.createDataFrame(rdd)
      checkRDD(df)
    }
  }

  it should "be deterministic with real data" in withQbeastContext() {
    withSpark { spark =>
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
  }

  it should "be deterministic when a row has only nullable columns" +
    " and null values" in withQbeastContext() {
      withSpark { spark =>
        val rdd = spark.sparkContext.parallelize(0.to(1000).map {
          case i if i % 3 == 0 => Client1(Some(i * i), None, Some(i))
          case i if i % 5 == 0 => Client1(Some(i * i), Some(s"student-$i"), None)
          case i if i % 7 == 0 => Client1(None, Some(s"student-$i"), Some(i))
          case i if i % 11 == 0 => Client1(None, None, None)
          case i => Client1(Some(i * i), Some(s"student-$i"), Some(i))
        })
        val df = spark.createDataFrame(rdd)
        checkRDD(df)
      }
    }

  it should "be deterministic when a row have only NOT nullable columns" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(0.to(1000).map(i => Client2(i * i, s"student-$i", i)))
      val df = spark.createDataFrame(rdd)
      checkRDD(df)

    }
  }

  def checkRDD(df: DataFrame): Unit =
    withQbeastContext() {
      val rev = SparkRevisionBuilder.createNewRevision(
        QTableID("test"),
        df,
        Map("columnsToIndex" -> df.columns.mkString(",")))

      val newDf = DoublePassOTreeDataAnalyzer invokePrivate addRandomWeight(df, rev)
      /* With less than 10k rows the probability of a collision is approximately 0.3%,
    show it should not happen  calculated with
    https://gist.github.com/benhoyt/b59c00fc47361b67bfdedc92e86b03eb#file-birthday_probability-py
       */

      val df2 = DoublePassOTreeDataAnalyzer invokePrivate addRandomWeight(df, rev)
      df2.agg(sum(col(weightColumnName))).first().get(0) shouldBe newDf
        .agg(sum(col(weightColumnName)))
        .first()
        .get(0)
    }

}
