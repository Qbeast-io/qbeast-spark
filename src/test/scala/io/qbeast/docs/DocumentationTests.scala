package io.qbeast.docs

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.AppendedClues.convertToClueful

class DocumentationTests extends QbeastIntegrationTestSpec {

  val config: SparkConf = sparkConfWithSqlAndCatalog.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

  behavior of "Documentation"

  it should "behave correctly Readme" in withSpark { spark =>
    withTmpDir { tmp_dir =>
      val csv_df = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("./src/test/resources/ecommerce100K_2019_Oct.csv")

      csv_df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "user_id,product_id")
        .option("cubeSize", 10000)
        .save(tmp_dir)

      val qbeast_df =
        spark.read
          .format("qbeast")
          .load(tmp_dir)

      qbeast_df.count() shouldBe csv_df
        .count() withClue "Readme count does not match the original"

      qbeast_df.schema shouldBe csv_df.schema withClue "Readme schema does not match the original"

    }
  }

  it should "behave correctly in Quickstart" in withExtendedSpark(config) { spark =>
    withTmpDir { qbeastTablePath =>
      val parquetTablePath = "s3a://qbeast-public-datasets/store_sales"

      val parquetDf = spark.read.format("parquet").load(parquetTablePath).na.drop()

      parquetDf.write
        .mode("overwrite")
        .format("qbeast") // Saving the dataframe in a qbeast datasource
        .option("columnsToIndex", "ss_cdemo_sk,ss_cdemo_sk") // Indexing the table
        .option("cubeSize", 300000)
        .save(qbeastTablePath)

      val qbeastDf = spark.read.format("qbeast").load(qbeastTablePath)

      qbeastDf.count() shouldBe parquetDf.count() withClue
        "Quickstart count does not match the original"
      qbeastDf.schema shouldBe parquetDf.schema withClue
        "Quickstart schema does not match the original"
    }
  }

  it should "behave correctly on Sample Pushdown Notebook" in withExtendedSpark(config) { spark =>
    withTmpDir { DATA_ROOT =>
      val parquet_table_path = "s3a://qbeast-public-datasets/store_sales"
      val qbeast_table_path = DATA_ROOT + "/qbeast/qtable"

      val parquet_df = spark.read.format("parquet").load(parquet_table_path)

      val processed_parquet_df =
        parquet_df
          .select("ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk")
          .na
          .drop()

      processed_parquet_df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "ss_cdemo_sk,ss_hdemo_sk")
        .option("cubeSize", 300000)
        .save(qbeast_table_path)

      val qbeast_df = spark.read.format("qbeast").load(qbeast_table_path)

      val deltaLog = DeltaLog.forTable(spark, qbeast_table_path)
      val totalNumberOfFiles = deltaLog.snapshot.allFiles.count()

      totalNumberOfFiles should be > 1L withClue
        "Total number of files in pushdown notebook changes to " + totalNumberOfFiles

      val query = qbeast_df.sample(0.1)
      val queryCount = query.count()
      val totalCount = qbeast_df.count()

      queryCount shouldBe totalCount / 10L +- totalCount / 100L withClue
        "The sample should be more or less 10%"
      import spark.implicits._

      val files = query.select(input_file_name()).distinct().as[String].collect()

      val numberOfFilesQuery = files.length.toLong
      numberOfFilesQuery should be < totalNumberOfFiles withClue
        "Number of files read in pushdown notebook changes to " + numberOfFilesQuery

      val numberOfRowsRead = spark.read.format("parquet").load(files: _*).count()

      numberOfRowsRead should be >= queryCount withClue
        "Number of rows read in pushdown notebook changes to " + numberOfRowsRead

    }
  }
}
