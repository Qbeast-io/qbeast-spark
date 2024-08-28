# Quickstart for Qbeast-spark

## Content

Welcome to the documentation of the Qbeast-Spark project.

In these sections you will find guides to better understand the technology behind an open format and be able to play with it in just a few lines of code. 


## Writing

Inside the project folder, launch a spark-shell with the required **dependencies**. The following example uses AWS S3 for reading from a **public** dataset.

>**ℹ️ Warning: Different cloud providers may require specific versions of Spark or Hadoop, or specific libraries. Refer [here](CloudStorages.md) to check compatibilities.** 

```bash
$SPARK_HOME/bin/spark-shell \
--packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
--conf spark.sql.extensions=io.qbeast.spark.internal.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
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

Read the ***ecommerce*** test dataset from [Kaggle](https://www.kaggle.com/code/adilemrebilgic/e-commerce-analytics/data).
```scala
val ecommerce = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("src/test/resources/ecommerce100K_2019_Oct.csv")
```

Indexing the data with the desired columns, in this case `user_id` and `product_id`.
```scala
val qbeastTablePath = "/tmp/qbeast-test-data/qtable"

(ecommerce.write
    .mode("overwrite")
    .format("qbeast")     // Saving the dataframe in a qbeast datasource
    .option("columnsToIndex", "user_id,product_id")      // Indexing the table
    .option("cubeSize", "500") // The desired number of records of the resulting files/cubes. Default is 5M
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

## SQL

Thanks to the `QbeastCatalog`, you can use plain SQL and `CREATE TABLE` or `INSERT INTO` in qbeast format.

To check the different configuration on the Catalog, please go to [Advanced Configuration](AdvancedConfiguration.md)

```scala
ecommerce.createOrReplaceTmpView("ecommerce_october")

spark.sql("CREATE OR REPLACE TABLE ecommerce_qbeast USING qbeast AS SELECT * FROM ecommerce_october")

//OR

val ecommerceNovember = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("./src/test/resources/ecommerce100K_2019_Nov.csv")
  
ecommerceNovember.createOrReplaceTmpView("ecommerce_november")

spark.sql("INSERT INTO ecommerce_qbeast SELECT * FROM ecommerce_november")
```
Sampling has also an operator called `TABLESAMPLE`, which can be expressed in both terms of rows or percentage. 

```scala
spark.sql("SELECT avg(price) FROM ecommerce_qbeast TABLESAMPLE(10 PERCENT)").show()
```

## Python

Similar to Scala and SQL, you can also use PySpark to operate on Qbeast tables. 
Here, you can find an end-to-end example following the same operations demonstrated earlier on this page.

```python
from pyspark.sql import SparkSession

# Initialize Spark session with Qbeast and Delta support, running locally
spark = SparkSession.builder \
    .appName("QbeastExample") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

qbeast_table_path = "/tmp/qbeast-test-data/qtable"

# Creating a temporary view for the ecommerce data of October
ecommerce_october = spark.read \
    .format("csv")\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("src/test/resources/ecommerce100K_2019_Oct.csv")

# Creating a Qbeast table using PySpark
ecommerce_october.write \
    .format("qbeast") \
    .mode("overwrite") \
    .option("columnsToIndex", "user_id,product_id") \
    .option("cubeSize", "500") \
    .save(qbeast_table_path)

# Reading the ecommerce data for November
ecommerce_november = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("src/test/resources/ecommerce300k_2019_Nov.csv")

# Inserting data into the Qbeast table using PySpark
ecommerce_november.write \
    .format("qbeast") \
    .mode("append") \
    .save(qbeast_table_path)

sampled_data = spark.read.format("qbeast").load(qbeast_table_path).sample(0.10)
sampled_data.selectExpr("avg(price)").show()
```

## Analyze and Optimize

Analyze and optimize the index to **announce** and **replicate** cubes respectively to maintain the tree's structure in an optimal state.

> WARNING: ISSUE #[282](https://github.com/Qbeast-io/qbeast-spark/issues/282) Replication not enabled on Multiblock Format (from v.0.6.0). We don't encourage to use this method since it can impact index consistency.

```scala
import io.qbeast.spark.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, qbeastTablePath)

// analyze the index structure and detect cubes that can be optimized
qbeastTable.analyze()

// optimize the index to improve read operations
qbeastTable.optimize()
```

# Other DML Operations

DML is a Data Manipulation Language that is used to manipulate data itself. For example: insert, update, and delete are instructions in SQL.

## Deletes

> WARNING: Data can be removed from a Qbeast table with Delta Lake API, but, as currently constructed, **it will leave the index in an inconsistent state.** See issue #[327](https://github.com/Qbeast-io/qbeast-spark/issues/327).

You can delete rows from a table using the `DeltaTable` API; then **the table should only be read using `delta`**.

```scala
import io.delta.tables._

val deltaTable = DeltaTable.forPath(spark, "/tmp/qbeast/people-10m")

// Declare the predicate by using a SQL-formatted string.
deltaTable.delete("birthDate < '1955-01-01'")

import org.apache.spark.sql.functions._
import spark.implicits._

// Declare the predicate by using Spark SQL functions and implicits.
deltaTable.delete(col("birthDate") < "1955-01-01")
```

## Table Tolerance (Work In Progress)

Specify the tolerance willing to accept and let qbeast handle the calculation for the optimal fraction size to use.

```scala
import io.qbeast.spark.implicits._

qbeastDf.agg(avg("user_id")).tolerance(0.1).show()
```