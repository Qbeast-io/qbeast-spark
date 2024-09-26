# Quickstart for Qbeast-spark

## Content

Welcome to the documentation of the Qbeast-Spark project.

In these sections you will find guides to better understand the technology behind an open format and be able to play with it in just a few lines of code.

Here's a summary of the topics covered in this document:

- [Pre-requisites](#pre-requisites)
  - [Download JAVA](#download-java)
  - [Download Apache Spark](#download-apache-spark)
- [Launch a Spark Shell](#launch-a-spark-shell)
- [Set up an Application](#set-up-an-application)
  - [Maven](#maven)
  - [SBT](#sbt)
  - [Python](#python)
  - [Advanced Spark Configuration](#advanced-spark-configuration)
- [Creating a Table](#creating-a-table)
  - [Python](#python-1)
  - [Scala](#scala)
  - [SQL](#sql)
  - [Advanced Table Configuration](#advanced-table-configuration)
- [Append](#append)
  - [Python](#python-2)
  - [Scala](#scala-1)
  - [SQL](#sql-1)
- [Read](#read)
  - [Python](#python-3)
  - [Scala](#scala-2)
  - [SQL](#sql-2)
- [QbeastTable API](#qbeasttable-api)
- [Optimize](#optimize)
- [Deletes](#deletes)
- [Cloud Providers Setup](#cloud-providers-setup)
  - [AWS](#aws)
  - [GCP](#gcp)
- [Index Visualizer](#index-visualizer)
- [Dependencies and Version Compatibility](#dependencies-and-version-compatibility)


## Pre-requisites

You can run the qbeast-spark library in two different ways:

1. Interactively: Start the Spark shell (Scala or Python) with Qbeast Spark and Delta Lake and run the code snippets.
2. As a Project: Set up a Maven or SBT project (Scala or Java), copy the code snippets into a source file, and run the project.

Before starting, ensure you have the following:

- **Java 8+**: Ensure that Java is installed and properly configured.
- **SBT/Gradle/Maven**: This is for managing dependencies if running in a development environment.
- **Apache Spark 3.5+**: A Spark installation with support for Scala 2.12

### Download JAVA

As mentioned in the official Apache Spark installation instructions [here](https://spark.apache.org/docs/latest/index.html#downloading), make sure you have a valid Java version installed (8, 11, or 17) and that Java is configured correctly on your system using either the system `PATH` or `JAVA_HOME` environmental variable.

Windows users should follow the instructions in this [blog](https://phoenixnap.com/kb/install-spark-on-windows-10), making sure to use the correct version of Apache Spark that is compatible with the latest versions of Delta Lake (3.2.0) and Qbeast Spark (0.7.0).

### Download Apache Spark

Download a compatible version of Apache Spark with Hadoop, and create the `SPARK_HOME` environment variable:

> ℹ️ Note: You can use Hadoop 2.7 if desired, but you could have some troubles with different cloud providers' storage, read more about it here.

```bash

wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

tar -xzvf spark-3.5.0-bin-hadoop3.tgz

export SPARK_HOME=$PWD/spark-3.5.0-bin-hadoop3
```

## Launch a Spark Shell

For running the code interactively, it's necessary to open a `pyspark` session in Python or a `spark-shell` Scala.

>**ℹ️ Warning: Different cloud providers may require specific versions of Spark or Hadoop, or specific libraries. Refer [here](CloudStorages.md) to check compatibilities.**

### Python

Install a `pyspark` version that is compatible with the latest version of Qbeast Spark:

```bash
pip install pyspark==<compatible-spark-version>
```

Run `pyspark` shell:

```bash
pyspark --packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
--conf spark.sql.extensions=io.qbeast.spark.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

### Scala

Run a `spark-shell` from the binaries:

```bash
$SPARK_HOME/bin/spark-shell \
--packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
--conf spark.sql.extensions=io.qbeast.spark.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

### SQL

```bash
$SPARK_HOME/bin/spark-sql \
--packages io.qbeast:qbeast-spark_2.12:0.7.0,io.delta:delta-spark_2.12:3.1.0 \
--conf spark.sql.extensions=io.qbeast.spark.QbeastSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

### Advanced Spark Configuration

Spark Configuration can help improving writing and reading performance. Here are a few configuration for qbeast. 


| **Configuration**                               | **Definition**                                               | **Default** |
|-------------------------------------------------|--------------------------------------------------------------|-------------|
| `spark.qbeast.index.defaultCubeSize`            | Default cube size for all datasets written in the session.   | 5000000     |
| `spark.qbeast.index.cubeWeightsBufferCapacity`  | Default buffer capacity for intermediate results.            | 100         |
| `spark.qbeast.index.columnsToIndex.auto`        | Automatically select columns to index.                       | false       |
| `spark.qbeast.index.columnsToIndex.auto.max`    | Maximum number of columns to index automatically.            | 10          |
| `spark.qbeast.index.numberOfRetries`            | Number of retries for writing data.                          | 2           |

Consult the [Qbeast-Spark advanced configuration](AdvancedConfiguration.md) for more information.

## Set up an Application

You can use the following Maven coordinates to build a project using Qbeast Spark binaries from Maven Central Repository.

### **Maven**

You include Qbeast Spark in your Maven project by adding it as a dependency in your `POM` file.

```xml
<dependency>
	<groupId>io.qbeast</groupId>
	<artifactId>qbeast-spark_2.12</artifactId>
	<version>0.7.0</version>
</dependency>
```

### **SBT**

You include Qbeast Spark in your SBT project by adding the following line to your `build.sbt` file:

```scala
libraryDependencies += "io.qbeast" %% "qbeast-spark" % "0.7.0"
```

To use a `SNAPSHOT` (NOT RECOMMENDED), add the Snapshots URL to the list of repositories:

```scala
ThisBuild / resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
```

### **Python**

To set up a Python project (for example, for unit testing), you can configure the `SparkSession` with the following options.

```python
import pyspark

# Already configured session

spark = pyspark.sql.SparkSession.builder.appName("MyApp").getOrCreate()

# Session with Configuration
pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.qbeast.spark.QbeastSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.qbeast.spark.internal.sources.catalog.QbeastCatalog").getOrCreate()

```

## Creating a Table

You can create a Table using Qbeast Layout with plain Spark APIs.

### Python

```python
data = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id: int, age:string")
data.write.mode("overwrite").option("columnsToIndex", "id,age").saveAsTable("qbeast_table")
```

### Scala

```scala
val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "age")
data.write.mode("overwrite").option("columnsToIndex", "id,age").saveAsTable("qbeast_table")
```

### SQL

```sql
CREATE TABLE qbeast_table (id INT, age STRING)
USING qbeast
OPTIONS ('columnsToIndex'='id,age');
```

#### With Location
It is possible to specify the location through the SQL:

```sql
CREATE TABLE qbeast_table (id INT, age STRING)
USING qbeast
LOCATION '/tmp/qbeast_table'
OPTIONS ('columnsToIndex'='id,age');
```

### Advanced Table Configuration

There are different options to fine-tune the underlying index.

| **Option**       | **Definition**                                                                                                                                                                                                                     | **Example**                                              |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `columnsToIndex` | Indicates the columns in the DataFrame used to index. We recommend selecting the variables more commonly queried to maximize the layout efficiency.                                                                                | `“id,age”`                                               |                                                                                                                                                                                                                                                                                                                                                                    |
| `cubeSize`       | Maximum amount of elements a cube should contain. Default is 5 million. It is a soft limit, which means that it can be exceeded. It is considered a bad sign that final sizes of the cubes duplicate or triplicate the `cubeSize`. | `1000`                                                   |                                                                                                                                                                                                          |
| `columnStats`    | Min and maximum values of the columns to index in JSON string. The space is computed at writing time, but if you know the stats in advance, it would skip that step and provide a more relevant index for your data.               | `"""{"a_min":0,"a_max":10,"b_min":20.0,"b_max":70.0}"""` |

### 

## Append

Append data to a path using DataFrame API in “append” mode, or SQL Insert Into clause.

### Python

#### Append to a Table
```python
append = spark.createDataFrame([(4, "d"), (5, "e")], "id: int, age:string")

# Save
append.write.\
  mode("append").\
  insertInto("qbeast_table")
```

#### Append to a Path
```python
append = spark.createDataFrame([(4, "d"), (5, "e")], "id: int, age:string")

# Save
append.write.\
  mode("append").\
  option("columnsToIndex", "id,age").\
  format("qbeast").\
  save("/tmp/qbeast_table")
```

### Scala

#### Append to a Table
```scala
val append = Seq((4, "d"), (5, "e")).toDF("id", "age")

// Save
append.write.
  mode("append").
  insertInto("qbeast_table")
```

#### Append to a Path
```scala
val append = Seq((4, "d"), (5, "e")).toDF("id", "age")

// Save
append.write.
  mode("append").
  option("columnsToIndex", "id,age").
  format("qbeast").
  save("/tmp/qbeast_table")
```

### SQL

Use **`INSERT INTO`** to add records to the new table. It will update the index in a **dynamic** fashion when new data is inserted.

```sql
INSERT INTO table qbeast_table VALUES (4, "d"), (5, "e");
```

## Read

Read data from a Qbeast Table by specifying the paths or the table name.

### Python

```python
qbeast_df = spark.read.format("qbeast").load("/tmp/qbeast_table")
```

### Scala

```scala
val qbeastDF = spark.read.format("qbeast").load("/tmp/qbeast_table")
```

### SQL

```sql
SELECT * FROM qbeast_table;
```
## Sampling

Sampling is the process of selecting a subset of data from a larger dataset to analyze and make inferences. It is beneficial because it **reduces computational costs**, **speeds up analysis**, and simplifies data handling while still **providing accurate and reliable insights** if the sample is representative.

Thanks to the [Qbeast Metadata](./QbeastFormat.md), it is possible to use the `sample` and `TABLESAMPLE` (in SQL) methods to **select a fraction of the data directly from storage** instead of loading and computing the results in memory with all the records. 
### Python

```python
qbeast_df.sample(0.3).show()
```

### Scala

```scala
qbeast_df.sample(0.3).show()
```

### SQL

```sql
SELECT * FROM qbeast_table TABLESAMPLE (30 PERCENT);
```

### Sampling Under The Hood
To check sampling perfomance, open your **Spark Web UI**, and observe how the sample operator is converted into a **filter** and pushed down to the source!
```scala
qbeastDf.sample(0.3).explain()
```

```
== Physical Plan ==
*(1) Filter ((qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) < -1717986918) AND (qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) >= -2147483648))
+- *(1) ColumnarToRow
+- FileScan parquet [ss_sold_time_sk#1088,ss_item_sk#1089,ss_customer_sk#1090,ss_cdemo_sk#1091,ss_hdemo_sk#1092,ss_addr_sk#1093,ss_store_sk#1094,ss_promo_sk#1095,ss_ticket_number#1096L,ss_quantity#1097,ss_wholesale_cost#1098,ss_list_price#1099,ss_sales_price#1100,ss_ext_discount_amt#1101,ss_ext_sales_price#1102,ss_ext_wholesale_cost#1103,ss_ext_list_price#1104,ss_ext_tax#1105,ss_coupon_amt#1106,ss_net_paid#1107,ss_net_paid_inc_tax#1108,ss_net_profit#1109,ss_sold_date_sk#1110] Batched: true, DataFilters: [(qbeast_hash(ss_cdemo_sk#1091, ss_cdemo_sk#1091, 42) < -1717986918), (qbeast_hash(ss_cdemo_sk#10..., Format: Parquet, Location: OTreeIndex[file:/tmp/qbeast-test-data/qtable], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<ss_sold_time_sk:int,ss_item_sk:int,ss_customer_sk:int,ss_cdemo_sk:int,ss_hdemo_sk:int,ss_a...

```

Notice that the sample operator is no longer present in the physical plan. It's converted into a `Filter (qbeast_hash)` instead and is used to select files during data scanning(`DataFilters` from `FileScan`). We skip reading many files in this way, involving less I/O.

## QbeastTable API

Get **insights** into the data using the `QbeastTable` interface available in Scala.

```scala
import io.qbeast.spark.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, "/tmp/qbeast_table")

qbeastTable.getIndexMetrics()
```

| **Method**                                           | **Definition**                                                                                                                                                                                                |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| revisionIDs(): List[Long]                            | Returns a list with all the Revision ID’s present in the Table.                                                                                                                                               |
| latestRevision(): Revision                           | Returns the Latest Revision available in the Table.                                                                                                                                                           |
| latestRevisionID(): Long                             | Returns the Latest Revision ID available in the Table.                                                                                                                                                        |
| revision(revisionID: Option[Int]): Revision          | Returns the `Revision` information of a particular Revision ID (if specified) or the latest one                                                                                                               |
| indexedColumns(revisionID: Option[Int]): Seq[String] | Returns the indexed column names of a particular Revision ID (if specified) or the latest one.                                                                                                                |
| cubeSize(revisionID: Option[Int])                    | Returns the Cube Size of a particular Revision ID (if specified) or the latest one.                                                                                                                           |
| getDenormalizedBlocks(revisionID: Option[Int])       | Get the Denormalized Blocks information of all files of a particular Revision ID (if specified) or the latest one.                                                                                            |
| getIndexMetrics(revisionID: Option[Int])             | Output the `IndexMetrics` information a particular Revision ID (if specified) or the latest one available. It is useful to know the state of the different levels of the index and the respective cube sizes. |

## Optimize

**Optimize** is an expensive operation that consists of **rewriting part of the files** to accomplish a **better layout** and **improve query performance**.

To minimize the write amplification of this command, **we execute it based on subsets of the table**, like `Revision ID's` or specific files.

> Read more about Revision and find an example here.
>

These are the 3 ways of executing the `optimize` operation:

```scala
// Optimizes the last Revision Available.
// This does NOT include previous Revision's optimizations.
qbeastTable.optimize()

// Optimizes the Revision number 2.
qbeastTable.optimize(2L)

// Optimizes the specific file
qbeastTable.optimize(Seq("file1", "file2"))
```

**If you want to optimize the full table, you must loop through `revisions`**:

```scala
// 1. Get all the Revision ID's available in the table.
val revisions = qbeastTable.revisionsIDs() 
// 2. For each revision, call the Optimize method
revisions.foreach(revision =>
  qbeastTable.optimize(revision)
)
```

Go to [QbeastTable documentation](https://github.com/Qbeast-io/qbeast-spark/blob/main/docs/QbeastTable.md) for more detailed information.

### Deletes

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

# Cloud Providers Setup
## AWS

For setting up writes and reads on Amazon S3 service, it is possible to use both private and public repositories.

> 🚧 Amazon Web Services S3 does not work with Hadoop 2.7. For this provider, you'll need Hadoop 3.2.

- If you are using a **public** bucket:

    ```bash
    $SPARK_HOME/bin/spark-shell \
    --conf spark.sql.extensions=io.qbeast.spark.QbeastSparkSessionExtension \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
    --packages io.qbeast:qbeast-spark_2.12:0.7.0,\
    io.delta:delta-spark_2.12:3.1.0,\
    com.amazonaws:aws-java-sdk:1.12.20,\
    org.apache.hadoop:hadoop-common:3.2.0,\
    org.apache.hadoop:hadoop-client:3.2.0,\
    org.apache.hadoop:hadoop-aws:3.2.0
    ```

- If you are using **private** buckets:

    ```bash
    $SPARK_HOME/bin/spark-shell \
    --conf spark.sql.extensions=io.qbeast.spark.QbeastSparkSessionExtension \
    --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
    --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}\
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --packages io.qbeast:qbeast-spark_2.12:0.7.0,\
    io.delta:delta-spark_2.12:3.1.0,\
    com.amazonaws:aws-java-sdk:1.12.20,\
    org.apache.hadoop:hadoop-common:3.2.0,\
    org.apache.hadoop:hadoop-client:3.2.0,\
    org.apache.hadoop:hadoop-aws:3.2.0
    ```

## GCP

Google has several services related to Qbeast including [Cloud Storage](https://cloud.google.com/storage), [BigQuery](https://cloud.google.com/bigquery), [BigLake](https://cloud.google.com/biglake), and [DataProc](https://cloud.google.com/dataproc) — perhaps more, this is the minimum for use with BigQuery using external tables.

### Manual Setup

1. Install the [GCS Cloud Storage connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) for Hadoop v3.
2. Provision one or more GCS Buckets.
3. Navigate to Google DataProc, select `Metastore Services -> Metastore` in the sidebar. Click `Create`, and configure the ***metastore config overrides***, setting: `hive.metastore.warehouse.dir: gs://<bucket name>/<nested path>/hive-warehouse` . The nested path is optional.
4. Selecting an existing Spark node (in GCE or GKE), and modify its properties to enable  Google Cloud & Qbeast configurations

    ```bash
    # Configure the Spark worker to use the Qbeast formatter library
    spark.sql.extensions io.qbeast.spark.QbeastSparkSessionExtension
    spark.sql.catalog.spark_catalog io.qbeast.spark.internal.sources.catalog.QbeastCatalog
    ```

5. Create a schema in BigQuery Studio in the same region than the GC bucket.
6. Create an external connection with `*connection` type* of `Apache Spark` , and configure to point to the DataProc metastore described in step #3.
7. Create an external connection for BiqQuery to address the Cloud Storage
    1. Click `Add`, Select `Connections to external data sources`, select `Vertex AI remote models, remote functions and BigLake (Cloud Resource)`, choose a connection ID, and select the region used for the GCS Bucket.
    2. Select the external connection created (matching the name) in the left sidebar, and copy the Service Account ID. Assign this service account ID permissions to the GCS Bucket by navigating to the bucket in Cloud Storage, `Grant Access`, entering the BQ Service Account as the principal, and assigning a `Storage Admin` role (to be refined later).
8. Create an external table within BigQuery targeting the Qbeast formatted table (using Delta Lake connector).

    ```sql
    CREATE EXTERNAL TABLE `<project>.<schema>.<table name>`
    WITH CONNECTION `<connection id>`
    OPTIONS (format ="DELTA_LAKE",  uris=['<bucket location>']);
    ```

## Index Visualizer

Use [Python index visualizer](https://github.com/Qbeast-io/qbeast-spark/blob/main/utils/visualizer/README.md) for your indexed table to visually examine the index structure and gather sampling metrics.

![image.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/f4a260f5-2594-4551-82be-296e3a418a69/26af38f1-6fcb-4637-a06b-80b28a9a65dd/image.png)

# Dependencies and Version Compatibility

| Version   | Spark     | Hadoop    | Delta Lake |
|-----------|-----------|-----------|------------|
| 0.1.0     | 3.0.0     | 3.2.0     | 0.8.0      |
| 0.2.0     | 3.1.x     | 3.2.0     | 1.0.0      |
| 0.3.x     | 3.2.x     | 3.3.x     | 1.2.x      |
| 0.4.x     | 3.3.x     | 3.3.x     | 2.1.x      |
| 0.5.x     | 3.4.x     | 3.3.x     | 2.4.x      |
| 0.6.x     | 3.5.x     | 3.3.x     | 3.1.x      |
| **0.7.x** | **3.5.x** | **3.3.x** | **3.1.x**  |

Check [here](https://docs.delta.io/latest/releases.html) for **Delta Lake** and **Apache Spark** version compatibility.


