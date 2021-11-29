package io.qbeast.docs

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.input_file_name

class DocumentationTests extends QbeastIntegrationTestSpec {

  behavior of "Documentation"

  it should "behave correctly Readme" in withExtendedSpark { spark =>
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

      assert(qbeast_df.count() == csv_df.count(), "Readme count does not match the original")
      assert(qbeast_df.schema == csv_df.schema, "Readme schema does not match the original")

    }
  }

  ignore should "behave correctly in Quickstart" in withExtendedSpark { spark =>
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

      assert(
        qbeastDf.count() == parquetDf.count(),
        "Quickstart count does not match the original")
      assert(qbeastDf.schema == parquetDf.schema, "Quickstart schema does not match the original")
    }
  }

  ignore should "behave correctly on Sample Pushdown Notebook" in withExtendedSpark { spark =>
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

      val processed_parquet_dir = DATA_ROOT + "/parquet/test_data"

      processed_parquet_df.write.mode("overwrite").format("parquet").save(processed_parquet_dir)

      val df = spark.read.format("parquet").load(processed_parquet_dir)
      val qbeast_df = spark.read.format("qbeast").load(qbeast_table_path)

      assert(
        qbeast_df.count() == df.count(),
        "Pushdown notebook count of indexed dataframe does not match the original")

      // Table changes?

      val deltaLog = DeltaLog.forTable(spark, qbeast_table_path)
      val totalNumberOfFiles = deltaLog.snapshot.allFiles.count()
      assert(
        totalNumberOfFiles == 21,
        "Total number of files in pushdown notebook changes to " + totalNumberOfFiles)

      val query = qbeast_df.sample(0.1)
      val numberOfFilesQuery = query.select(input_file_name()).distinct().count()
      assert(
        numberOfFilesQuery == 1,
        "Number of files read in pushdown notebook changes to " + numberOfFilesQuery)

      val file = query.select(input_file_name()).distinct().head().getString(0)
      val numberOfRowsRead = spark.read.format("parquet").load(file).count()
      assert(
        numberOfRowsRead == 302715,
        "Number of rows read in pushdown notebook changes to " + numberOfRowsRead)

    }
  }
}
