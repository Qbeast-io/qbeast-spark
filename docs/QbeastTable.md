# QbeastTable

This API is implemented to interact with your QbeastTable.

Creating an instance of QbeastTable is as easy as:

```scala
import io.qbeast.spark.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, "path/to/qbeast/table")
```
## Table Information

If you want to know more about the Format, you can use the different get methods. 

>:information_source: **Note: In each of them you can specify the particular `RevisionID` you want to get the information from.**

```scala
qbeastTable.indexedColumns() // current indexed columns

qbeastTable.cubeSize() // current cube size

qbeastTable.revisionsIDs() // all the current Revision identifiers

qbeatsTable.lastRevisionID() // the last Revision identifier
```

## Table Operations
Through QbeastTable you can also execute `Analyze`, `Optimize` and `Compact` operations, which are **currently experimental**.

- **`Analyze`**: analyzes the index **searching for possible optimizations**.
- **`Optimize`**: optimize the index parts analyzed in the previous operation. The goal is to **improve reading performance** by accessing the less amount of data possible.
- **`Compact`**: **rearranges** index information that is stored into **small files**. Compaction will **reduce the number of files** when you have many writing operations on the table.
```scala
qbeastTable.analyze() // returns the Serialized cube ID's to optimize

qbeastTable.optimize() // optimizes the cubes

qbeastTable.compact() // compacts small files into bigger ones
```

## Index Metrics

`IndexMetrics` aims to provide an overview for a given revision of the index.

You can use it during development to compare different indexes built using different indexing parameters such as the `desiredCubeSize` and `columnsToIndex`.

This is meant to be used as an easy access point to analyze the resulting index, which should come handy for comparing different index parameters or even implementations.

```scala
val metrics = qbeastTable.getIndexMetrics()

println(metrics)
```

```
// EXAMPLE OUTPUT

OTree Index Metrics:
dimensionCount: 2
elementCount: 2879966589
depth: 8
cubeCount: 13141
desiredCubeSize: 500000
indexingColumns: ss_sold_date_sk,ss_item_sk
avgFanout: 4.0
depthOnBalance: 1.206019253488489

Stats on cube sizes:
Quartiles:
- min: 456367
- 1stQ: 498510
- 2ndQ: 499954
- 3rdQ: 501410
- max: 536430
Stats:
- count: 3285
- l1_dev: 0.00449603896499239
- l2_dev: 1.3487574366807247E-4
Level-wise stats:
level, avgCubeSize, stdCubeSize, cubeCount, avgWeight:
- 0:	497810,		4442,		1,	1.7361319627929786E-4
- 1:	494798,		6480,		4,	8.689350799817908E-4
- 2:	499781,		3871,		16,	0.003668841950401859
- 3:	500516,		3899,		64,	0.015534089088738918
- 4:	500289,		3876,		256,	0.06698862054431544
- 5:	499966,		3865,		1024,	0.287867372830027
- 6:	499962,		3865,		1792,	0.6729941083529944
- 7:	500142,		3867,		128,	0.7959112180321912
```

## Metrics
### 1. General index metadata:

- **dimensionCount**: the number of dimensions (indexed columns) in the index
- **elementCount**: the number of records for this revision
- **desiredCubeSize**: the desired cube size chosen at the moment of indexing
- **Number of cubes**: the number of nodes in the index tree
- **depth**: the number of levels in the tree
- **avgFanOut**: the average number of children per non-leaf cube. The max value for this metrics is `2 ^ dimensionCount`
- **depthOnBalance**: how far the depth of the tree is from the theoretical value, assuming all inner cubes have max fan out
- **indexingColumns**: the indexing column names

### 2. Cube sizes stats:
Meant to describe the distribution of cube sizes:
- `metrics.innerCubeSizeMetrics` for inner cubes. `metrics.leafCubeSizeMetrics` for leaf cubes
- **min**, **max**, **quartiles**, and how far the cube sizes are from the `desiredCubeSize`(**l1 and l2 error**).
- The average normalizedWeight, cube size, count, and standard deviation per level.

### 3. `Map[CubeId, CubeStatus]`
- More information can be extracted from the index tree through `metrics.cubeStatuses`
