# Advanced configurations

There's different configurations for the index that can affect the performance on read or the writing process. Here is a resume of some of them.

## Catalogs

We designed the `QbeastCatalog` to work as an **entry point for other format's Catalog's** as well. 

However, you can also handle different Catalogs simultaneously.

### 1. Unified Catalog

```bash
--conf spark.sql.catalog.spark_catalog=io.qbeast.catalog.QbeastCatalog
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
--conf spark.sql.catalog.qbeast_catalog=io.qbeast.catalog.QbeastCatalog
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

## Indexing via Quantile Based CDF

> **WARNING**: This is an **Experimental Feature**, and the API might change in the near future.

The default column transformation for Strings (`HashTransformation`) has limited range query supports since the lexicographic ordering of the String values are not preserved. On the numeric side, the default transformation is `LinearTransformation`, which is a simple linear transformation that preserves the ordering of the values.

This can be addressed by introducing a custom Quantile Based sequence in the form of sorted `Seq`, and can lead to several improvements including:
1. more efficient file-pruning because of its reduced file-level column min/max
2. support for range queries on String columns
3. improved overall query speed

The following code snippet demonstrates the extraction of a Quantile-based CDF from the source data:

```scala
import io.qbeast.utils.QbeastUtils

val columnQuantiles = QbeastUtils.computeQuantilesForColumn(df, "brand")
val columnStats = s"""{"brand_quantiles":$columnQuantiles}"""

(df
  .write
  .mode("overwrite")
  .format("qbeast")
  .option("columnsToIndex", "brand:quantiles")
  .option("columnStats", columnStats)
  .save("/tmp/qbeast_table_quantiles"))
```

This is only necessary for the first write, if not otherwise made explicit, all subsequent appends will reuse the same quantile calculation.
Any new custom quantiles provided during `appends` forces the creation of a new `Revision`.

### How to configure quantiles computation
The `computeQuantilesForColumn` method computes the quantiles for the specified column and returns a `Seq` of quantile values. The `Seq` is then serialized into a `String` and passed as a custom column transformation to the `columnsToIndex` option.

You can **tune the number of quantiles and the relative error** for numeric columns using the QbeastUtils API.
```scala
val columnQuantiles =
  QbeastUtils.computeQuantilesForColumn(df = df, columnName = columnName)
val columnQuantilesNumberOfQuantiles =
  QbeastUtils.computeQuantilesForColumn(df = df, columnName = columnName, numberOfQuantiles = 100)
// For numeric columns, you can also specify the relative error
// For String columns, the relativeError is ignored
val columnQuantilesRelativeError =
  QbeastUtils.computeQuantilesForColumn(df = df, columnName = columnName, relativeError = 0.3)
val columnQuantilesNumAndError =
  QbeastUtils.computeQuantilesForColumn(df = df, columnName = columnName, numberOfQuantiles = 100, relativeError = 0.3)
```
## DefaultCubeSize

If you don't specify the cubeSize at DataFrame level, the default value is used. This is set to 5M, so if you want to change it
for testing or production purposes, you can do it through Spark Configuration:

```shell
--conf spark.qbeast.index.defaultCubeSize=100000
```

## CubeDomainsBufferCapacity

The parallel indexing algorithm can be divided into two stages: the estimation of index metadata and its usage to assign rows to cubes.
During the first stage, separate smaller OTrees are built in parallel, and their metadata (cube domains) are extracted and merged into a single index.

Spark intrinsically divides the dataset into partitions, and all records in a partition are used to build a local tree.
If your spark partitions are too large to fit in memory, `CubeDomainsBufferCapacity` can be used to limit the buffer size when building local trees.

If the partition has 1000 records and the buffer capacity is 100. You will create 10 local trees, each with 100 records.

This value is also used to determine the group cube size, i.e., the cube size of the local trees:
```
numGroups = MAX(numPartitions, (numElements / cubeWeightsBufferCapacity))
groupCubeSize = desiredCubeSize / numGroups
```

You can change this number through the Spark Configuration:

```shell
--conf spark.qbeast.index.cubeDomainsBufferCapacity=10000
```

## NumberOfRetries

You can change the number of retries for the LocalKeeper in order to test it. 

```shell
--conf spark.qbeast.index.numberOfRetries=10000
```

## Pre-commit Hooks
**Pre-commit hooks** enable the execution of custom code just before a write or optimization is committed.

To implement such hooks, extend `io.qbeast.spark.delta.hook.PreCommitHook` by implementing its `run` method, which has access to the sequence of `Action`s created by the operation.
The same method returns a `Map[String, String],` which will be used as `tags` for the transaction's `CommitInfo`:

```json
{
  "commitInfo": {
    "timestamp": 1718787341410,
    "operation": "WRITE",
    ...
    "tags": {
      "HookOutputKey": "HookOutputValue"
    },
    ...
  }
}
```

1. You can use more than one hook, as shown in the case below: `myHook1`, and `myHook2.`
2. For each hook you want to use, provide their class names with the option name: `qbeastPreCommitHook.<custom-hook-name>.`
3. Add an option with the name `qbeastPreCommitHook.<custom-hook-name>.arg` for the ones that take initiation arguments. Currently, only one `String` argument is allowed for each hook.

```scala
// Hooks for Writes
df
  .write
  .format("qbeast")
  .option("qbeastPreCommitHook.myHook1", classOf[SimpleHook].getCanonicalName)
  .option("qbeastPreCommitHook.myHook2", classOf[StatefulHook].getCanonicalName)
  .option("qbeastPreCommitHook.myHook2.arg", myStringHookArg)
  .save(pathToTable)
```

```scala
// Hooks for Optimizations
import io.qbeast.table.QbeastTable
val qt = QbeastTable.forPath(spark, tablePath)
val options = Map(
  "qbeastPreCommitHook.myHook1" -> classOf[SimpleHook].getCanonicalName,
  "qbeastPreCommitHook.myHook2" -> classOf[StatefulHook].getCanonicalName,
  "qbeastPreCommitHook.myHook2.arg" -> "myStringHookArg"
)
qt.optimize(filesToOptimize, options)
```

## Advanced Index metadata analysis
In addition to the IndexMetrics class, we provide handy access to the low-level details of the Qbeast index through the  
QbeastTable.forTable(sparkSession, tablePath) methods that returns a Dataset[DenormalizedBlock] which
contains all indexed metadata in an easy-to-analyze format.

```scala
import io.qbeast.table.QbeastTable
val qt = QbeastTable.forPath(spark, tablePath)
val dnb = qt.getDenormalizedBlocks()
dnb.select("filePath").distinct.count() // number of files
dnb.count() // number of blocks
dnb.groupBy("filePath").count().orderBy(col("count").desc).show() // Show the files with the most blocks
dnb.groupBy("cubeId").count().orderBy(col("count").desc).show()  // Show the cubeId with the most blocks
```