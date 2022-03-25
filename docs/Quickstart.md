# Quickstart for Qbeast-spark

## Content

Welcome to the documentation of the Qbeast-Spark project.

In these sections you will find guides to better understand the technology behind an open format and be able to play with it in just a few lines of code. 


## Writing

Inside the project folder, launch a spark-shell with the required **dependencies**. The following example uses AWS S3 for reading from a **public** dataset.

>**ℹ️ Warning: Different cloud providers may require specific versions of Spark or Hadoop, or specific libraries. Refer [here](CloudStorages.md) to check compatibilities.** 

```bash
$SPARK_HOME/bin/spark-shell \
--jars ./target/scala-2.12/qbeast-spark-assembly-0.2.0.jar \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
--packages io.delta:delta-core_2.12:1.0.0,\
com.amazonaws:aws-java-sdk:1.12.20,\
org.apache.hadoop:hadoop-common:3.2.0,\
org.apache.hadoop:hadoop-client:3.2.0,\
org.apache.hadoop:hadoop-aws:3.2.0
```
As an **_extra configuration_**, you can also change two global parameters of the index:

1. The **default desired size** of the written files (100000)
```
--conf spark.qbeast.index.defaultCubeSize=200000
```
2. The **default buffer capacity for intermediate results** (100000)

```
--conf spark.qbeast.index.cubeWeightsBufferCapacity=200
```
Consult the [Qbeast-Spark advanced configuration](AdvancedConfiguration.md) for more information.

Read the ***store_sales*** public dataset from `TPC-DS`, the table has with **23** columns in total and was generated with a `scaleFactor` of 1. Check [The Making of TPC-DS](http://www.tpc.org/tpcds/presentations/the_making_of_tpcds.pdf) for more details on the dataset.

```scala
val parquetTablePath = "s3a://qbeast-public-datasets/store_sales"

val parquetDf = spark.read.format("parquet").load(parquetTablePath).na.drop()
```

Indexing the data with the desired columns, in this case `ss_cdemo_sk` and `ss_cdemo_sk`.
```scala
val qbeastTablePath = "/tmp/qbeast-test-data/qtable"

(parquetDf.write
    .mode("overwrite")
    .format("qbeast")     // Saving the dataframe in a qbeast datasource
    .option("columnsToIndex", "ss_cdemo_sk,ss_cdemo_sk")      // Indexing the table
    .option("cubeSize", 300000) // The desired number of records of the resulting files/cubes. Default is 100000
    .save(qbeastTablePath))
```

## Sampling

Allow the sample operator to be pushed down to the source when sampling, reducing i/o and computational cost.

Perform sampling, open your **Spark Web UI**, and observe how the sample operator is converted into a **filter** and pushed down to the source!
```scala
val qbeastDf = (spark
  .read
  .format("qbeast")
  .load(qbeastTablePath))

qbeastDf.sample(0.1).explain()
```
```
== Physical Plan ==
*(1) Filter ((qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) < -1717986918) AND (qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) >= -2147483648))
+- *(1) ColumnarToRow
+- FileScan parquet [ss_sold_time_sk#1088,ss_item_sk#1089,ss_customer_sk#1090,ss_cdemo_sk#1091,ss_hdemo_sk#1092,ss_addr_sk#1093,ss_store_sk#1094,ss_promo_sk#1095,ss_ticket_number#1096L,ss_quantity#1097,ss_wholesale_cost#1098,ss_list_price#1099,ss_sales_price#1100,ss_ext_discount_amt#1101,ss_ext_sales_price#1102,ss_ext_wholesale_cost#1103,ss_ext_list_price#1104,ss_ext_tax#1105,ss_coupon_amt#1106,ss_net_paid#1107,ss_net_paid_inc_tax#1108,ss_net_profit#1109,ss_sold_date_sk#1110] Batched: true, DataFilters: [(qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) < -1717986918), (qbeast_hash(ss_cdemo_sk#10..., Format: Parquet, Location: OTreeIndex[file:/tmp/qbeast-test-data/qtable], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ss_sold_time_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_cdemo_sk:int,ss_hdemo_sk:int,ss_a...

```

Notice that the sample operator is no longer present in the physical plan. It's converted into a `Filter (qbeast_hash)` instead and is used to select files during data scanning(`DataFilters` from `FileScan`). We skip reading many files in this way, involving less I/O.


## Analyze and Optimize

Analyze and optimize the index to **announce** and **replicate** cubes respectively to maintain the tree's structure in an optimal state.

```scala
import io.qbeast.spark.table._

val qbeastTable = QbeastTable.forPath(spark, qbeastTablePath)

// analyze the index structure and detect cubes that can be optimized
qbeastTable.analyze()

// optimize the index to improve read operations
qbeastTable.optimize()
```

See [OTreeAlgorithm](OTreeAlgorithm.md) and [QbeastFormat](QbeastFormat.md) for more details.

## Table Tolerance (Work In Progress)

Specify the tolerance willing to accept and let qbeast handle the calculation for the optimal fraction size to use.

```scala
import io.qbeast.spark.implicits._

qbeastDf.agg(avg("user_id")).tolerance(0.1).show()
```