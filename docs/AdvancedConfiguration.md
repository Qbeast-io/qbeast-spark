# Advanced configurations

There's different configurations for the index that can affect the performance on read or the writing process. Here is a resume of some of them.

## Catalogs

We designed the `QbeastCatalog` to work as an **entry point for other format's Catalog's** as well. 

However, you can also handle different Catalogs simultanously.

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

## CubeSize

CubeSize option lets you specify the maximum size of the cube, in number of records. By default, it's set to 5M.

```scala
df.write.format("qbeast").option("cubeSize", "10000")
```

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
--conf spark.qbeast.compact.minFileSize=1\
--conf spark.qbeast.compact.maxFileSize=10000
```
