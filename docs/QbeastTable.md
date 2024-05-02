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
Through `QbeastTable` you can also execute `Optimize` operation. This command is used to **optimize the files in the table**, merging those small blocks into bigger ones to maintain an even distribution.

These are the 3 ways of executing the `optimize` operation:

```scala

qbeastTable.optimize() // Optimizes the last Revision Available.
// This does NOT include previous Revision's optimizations.

qbeastTable.optimize(2L) // Optimizes the Revision number 2.

qbeastTable.optimize(Seq("file1", "file2")) // Optimizes the specific files

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
depth: 9
cubeCount: 13141
desiredCubeSize: 500000
indexingColumns: ss_sold_date_sk,ss_item_sk
avgFanout: 4.0
depthOnBalance: 1.3567716601745503

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
- 0:	497810,		0,		1,	1.7361319627929786E-4
- 1:	494798,		3550,		4,	8.689350799817908E-4
- 2:	499781,		3488,		16,	0.003668841950401859
- 3:	500516,		4292,		64,	0.015534089088738918
- 4:	500289,		3967,		256,	0.06698862054431544
- 5:	499966,		3530,		1024,	0.287867372830027
- 6:	499962,		3040,		1792,	0.6729941083529944
- 7:	500142,		10508,		128,	0.7959112180321912
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
