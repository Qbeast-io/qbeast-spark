/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.context.QbeastContext
import io.qbeast.spark.index.OTreeAlgorithmTest._
import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.model.{ColumnInfo, RevisionUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
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
        checkRDD(df, df.columns)
      }
    }

  it should "be deterministic when a row have only strings" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd = spark.sparkContext.parallelize(
        0.to(1000).map(i => ClientString((i * i).toString, s"student-$i", i.toString)))
      val df = spark.createDataFrame(rdd)
      checkRDD(df, df.columns)
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
      checkRDD(df, df.columns)
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
      checkRDD(df, df.columns)
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
        checkRDD(df, df.columns)
      }
    }

  it should "be deterministic when a row have only NOT nullable columns" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(0.to(1000).map(i => Client2(i * i, s"student-$i", i)))
      val df = spark.createDataFrame(rdd)
      checkRDD(df, df.columns)

    }
  }

  def checkRDD(df: DataFrame, names: Seq[String]): Unit =
    withQbeastContext() {

      val newDf = QbeastContext.oTreeAlgorithm invokePrivate addRandomWeight(df, names)
      /* With less than 10k rows the probability of a collision is approximately 0.3%,
    show it should not happen  calculated with
    https://gist.github.com/benhoyt/b59c00fc47361b67bfdedc92e86b03eb#file-birthday_probability-py
       */

      val df2 = QbeastContext.oTreeAlgorithm invokePrivate addRandomWeight(df, names)
      df2.agg(sum(col(weightColumnName))).first().get(0) shouldBe newDf
        .agg(sum(col(weightColumnName)))
        .first()
        .get(0)
    }

  "ColumnInfo.get" should "get correct values with not nullable numbers" in withQbeastContext(
  ) {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(
          0.to(100)
            .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))
      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2", "val3")
      ColumnInfo.get(df, names) shouldBe Seq(
        ColumnInfo("age", 0.0, 100.0),
        ColumnInfo("val2", 123.0, 100123.0),
        ColumnInfo("val3", 0.0, 256734.32143))

    }
  }

  it should "get correct values with nullable numbers" in withQbeastContext() {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(0
          .to(100)
          .map(i =>
            Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143))))
      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2", "val3")
      ColumnInfo.get(df, names) shouldBe Seq(
        ColumnInfo("age", 0.0, 100.0),
        ColumnInfo("val2", 123.0, 100123.0),
        ColumnInfo("val3", 0.0, 256734.32143))
    }
  }

  it should "get correct values with some nulls" in withQbeastContext() {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(0
          .to(100)
          .map {
            case i if i % 2 == 0 =>
              Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143))
            case i if i % 3 == 0 =>
              Client4(i * i, s"student-$i", None, Some(i * 1000 + 123), Some(i * 2567.3432143))
            case i =>
              Client4(i * i, s"student-$i", None, None, None)
          })
      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2", "val3")
      ColumnInfo.get(df, names) shouldBe Seq(
        ColumnInfo("age", 0.0, 100.0),
        ColumnInfo("val2", 123.0, 100123.0),
        ColumnInfo("val3", 0.0, 256734.32143))
    }
  }

  it should "handle null columns like max 0 min 0" in withQbeastContext() {
    withSpark { spark =>
      val rdd =
        spark.sparkContext.parallelize(
          0
            .to(100)
            .map(i => Client4(i * i, s"student-$i", Some(i), None, None)))
      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2", "val3")
      ColumnInfo.get(df, names) shouldBe Seq(
        ColumnInfo("age", 0.0, 100.0),
        ColumnInfo("val2", 0.0, 0),
        ColumnInfo("val3", 0, 0))
    }
  }

  "isInRevision" should
    "detect a new revision when the space grows" in withSpark { spark =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val rdd =
          spark.sparkContext.parallelize(
            0
              .to(100)
              .map(i =>
                Client4(
                  i * i,
                  s"student-$i",
                  Some(i),
                  Some(i * 1000 + 123),
                  Some(i * 2567.3432143))))
        val df = spark.createDataFrame(rdd)
        val names = List("age", "val2", "val3")

        val (_, revision, _) = oTreeAlgorithm.indexFirst(df, names)

        val df2 = df.withColumn("val2", expr("val2 * 3"))

        assert(!RevisionUtil.revisionContains(revision, df2, names))
      }

    }

  it should "maintain old revision when data fits" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      val rdd =
        spark.sparkContext.parallelize(0
          .to(100)
          .map(i =>
            Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143))))
      val df = spark.createDataFrame(rdd)
      val names = List("age", "val2", "val3")

      val (_, revision, _) = oTreeAlgorithm.indexFirst(df, names)

      val df2 = df.withColumn("val2", expr("val2 + 3"))

      assert(RevisionUtil.revisionContains(revision, df2, names))
    }

  }

}
