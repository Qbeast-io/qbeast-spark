# Advanced configurations

There's different configurations for the index that can affect the performance on read or the writing process. Here is a resume of some of them.

## Catalogs

We designed the `QbeastCatalog` to work as an **entry point for other format's Catalog's** as well. 

However, you can also handle different Catalogs simultaneously.

### 1. Unified Catalog

```bash
--conf spark.sql.catalog.spark_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

Using the `spark_catalog` configuration, you can write **qbeast** and **delta** ( or upcoming formats ;) ) tables into the `default` namespace.

```scala
df.write
  .format("qbeast")
  .option("columnsToIndex", "user_id,product_id")
  .saveAsTable("qbeast_table")

df.write
  .format("delta")
  .saveAsTable("delta_table")
```
### 2. Secondary catalog

For using **more than one Catalog in the same session**, you can set it up in a different space. 

```bash
--conf spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalog.qbeast_catalog=io.qbeast.spark.internal.sources.catalog.QbeastCatalog
```

Notice the `QbeastCatalog` conf parameter is not anymore `spark_catalog`, but has a customized name like `qbeast_catalog`. Each table written using the **qbeast** implementation, should have the prefix `qbeast_catalog`. 

For example:

```scala
// DataFrame API
df.write
  .format("qbeast")
  .option("columnsToIndex", "user_id,product_id")
  .saveAsTable("qbeast_catalog.default.qbeast_table")

// SQL
spark.sql("CREATE TABLE qbeast_catalog.default.qbeast_table USING qbeast AS SELECT * FROM ecommerce")
```



## ColumnsToIndex

These are the columns you want to index. Try to find those which are interesting for your queries, or your data pipelines. 

You can specify different advanced options to the columns to index:

- **Type**: The type of the index you want to create in that column. Can be linear (numeric) or hash (string). By default, it would use the type of data.


```scala
df.write.format("qbeast").option("columnsToIndex", "column:type,column2:type...")
```

## Automatic Column Selection

To **avoid specifying the `columnsToIndex`**, you can enable auto indexer through the Spark Configuration:

```shell
--conf spark.qbeast.index.columnsToIndex.auto=true \
--conf spark.qbeast.index.columnsToIndex.auto.max=10
```
And write the DataFrame without any extra option:

```scala
df.write.format("qbeast").save("path/to/table")
```

Read more about it in the [Columns to Index selector](ColumnsToIndexSelector.md) section.

## CubeSize

CubeSize option lets you specify the maximum size of the cube, in number of records. By default, it's set to 5M.

```scala
df.write.format("qbeast").option("cubeSize", "10000")
```

## ColumnStats

One feature of the Qbeast Format is the `Revision`. 

This `Revision` contains some **characteristics of the index**, such as columns to index or cube size. But it also **saves the info of the space that you are writing** (min and maximum values of the columns). 

This space is computed based on the dataset that is currently being indexed, and **if you append records that fall outside this space will trigger another `Revision`**.

Having many `Revision` could be painful for reading process, since we have to query each one separately. 

To avoid that, you can tune the space dimensions by adding the `columnStats` option.

```scala
df.write.format("qbeast")
.option("columnsToIndex", "a,b")
.option("columnStats","""{"a_min":0,"a_max":10,"b_min":20.0,"b_max":70.0}""")
.save("/tmp/table")
```
In a `JSON` string, you can pass the **minimum and maximum values of the columns you are indexing** with the following schema:
```json
{
  "columnName_min" : value
  "columnName_max" : value

}
```

## TxnAppId and TxnVersion

These options are used to make the writes idempotent.

The option `txnAppId` identifies an application writing data to the table. It is
the responsibility of the user to assign unique identifiers to the applications
writing data to the table.

The option `txnVersion` identifies the transaction issued by the application.
The value of this option must be a valid string representation of a positive
long number.

```scala
df.write.format("qbeast")
.option("columnsToIndex", "a")
.option("txnAppId", "ingestionService")
.option("txnVersion", "1")
```

If the table already contains the data written by some other transaction with
the same `txnAppId` and `txnVersion` then the requested write will be ignored.

```scala
// The data is written
df.write.format("qbeast")
.option("columnsToIndex", "a")
.option("txnAppId", "ingestionService")
.option("txnVersion", "1")
...
// The data is ignored
df.write.format("qbeast")
.mode("append")
.option("txnAppId", "ingestionService")
.option("txnVersion", "1")
```

## Indexing Timestamps with ColumnStats

For indexing `Timestamps` or `Dates` with `columnStats` (min and maximum ranges), notice that **the values need to be formatted in a proper way** (following `"yyyy-MM-dd HH:mm:ss.SSSSSS'Z'"` pattern) for Qbeast to be able to parse it. 

Here's a snippet that would help you to codify the dates:

```scala
val minTimestamp = df.selectExpr("min(date)").first().getTimestamp(0)
val maxTimestamp = df.selectExpr("max(date)").first().getTimestamp(0)
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
val columnStats =
  s"""{ "date_min":"${formatter.format(minTimestamp)}",
     |"date_max":"${formatter.format(maxTimestamp)}" }""".stripMargin
```

## String indexing via Histograms

The default **String** column transformation (`HashTransformation`) has limited range query supports since the lexicographic ordering of the String values are not preserved.

This can be addressed by introducing a custom **String** histogram in the form of sorted `Seq[String]`, and can lead to several improvements including:
1. more efficient file-pruning because of its reduced file-level column min/max
2. support for range queries on String columns
3. improved overall query speed

The following code snippet demonstrates the extraction of a **String** histogram from the source data:
```scala
 import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions
 import org.apache.spark.sql.DataFrame
 import org.apache.spark.sql.functions.{col, min}

 def getStringHistogramStr(columnName: String, numBins: Int, df: DataFrame): String = {
   val binStarts = "__bin_starts"
   val stringPartitionColumn =
     MultiDimClusteringFunctions.range_partition_id(col(columnName), numBins)
   df
     .select(columnName)
     .distinct()
     .na.drop()
     .groupBy(stringPartitionColumn)
     .agg(min(columnName).alias(binStarts))
     .select(binStarts)
     .orderBy(binStarts)
     .collect()
     .map { r =>
       val s = r.getAs[String](0)
       s"'$s'"
     }
     .mkString("[", ",", "]")
 }

val brandStats = getStringHistogramStr("brand", 50, df)
val statsStr = s"""{"brand_histogram":$brandStats}"""

(df
  .write
  .mode("overwrite")
  .format("qbeast")
  .option("columnsToIndex", "brand:histogram")
  .option("columnStats", statsStr)
  .save(targetPath))
```
This is only necessary for the first write, if not otherwise made explicit, all subsequent appends will reuse the same histogram.
Any new custom histogram provided during `appends` forces the creation of a new `Revision`.

A default **String** histogram("a" t0 "z") will be used if the use of histogram is stated(`stringColName:string_hist`) with no histogram in `columnStats`.
The default histogram can not supersede an existing `StringHashTransformation`.

## DefaultCubeSize

If you don't specify the cubeSize at DataFrame level, the default value is used. This is set to 5M, so if you want to change it
for testing or production purposes, you can do it through Spark Configuration:

```shell
--conf spark.qbeast.index.defaultCubeSize=100000
```

## CubeWeightsBufferCapacity

### Trade-off between memory pressure and index quality

The current indexing algorithm uses a greedy approach to estimate the data distribution without additional shuffling.
Still, there's a tradeoff between the goodness of such estimation and the memory required during the computation.
The cubeWeightsBufferCapacity property controls such tradeoff by defining the maximum number of elements stored in
a memory buffer when indexing. It basically follows the next formula, which you can see in the method
`estimateGroupCubeSize()` from `io.qbeast.core.model.CubeWeights.scala`:
```
numGroups = MAX(numPartitions, (numElements / cubeWeightsBufferCapacity))
groupCubeSize = desiredCubeSize / numGroups
```

As you can infer from the formula, the number of working groups used when scanning the dataset influences the quality
of the data distribution. A lower number of groups will result in a higher index precision, while having more groups
and fewer elements per group will lead to worse indexes.

You can change this number through the Spark Configuration:

```shell
--conf spark.qbeast.index.cubeWeightsBufferCapacity=10000
```

## NumberOfRetries

You can change the number of retries for the LocalKeeper in order to test it. 

```shell
--conf spark.qbeast.index.numberOfRetries=10000
```

## Min/Max file size for compaction

You can set the minimum and maximum size of your files for the compaction process.

```shell
--conf spark.qbeast.compact.minFileSizeInBytes=1 \
--conf spark.qbeast.compact.maxFileSizeInBytes=10000
```

## Data Staging
You can set up the `SparkSession` with a **data staging area** for all your Qbeast table writes.

A staging area is where you can put the data you don't yet want to index but still want to be available for your queries.
To activate staging, set the following configuration to a non-negative value.

```scala
--conf spark.qbeast.index.stagingSizeInBytes=1000000000
```
When the staging area is not full, all writes are staged without indexing(written in `delta`).
When the staging size reaches the defined value, the current data is merged with the staged data and written at once.

The feature can be helpful when your workflow does frequent small appends. Setting up a staging area makes sure that all index appends are at least of the staging size.

We can empty the staging area with a given write by setting the staging size to `0`:
```scala
--conf spark.qbeast.index.stagingSizeInBytes=0
```
