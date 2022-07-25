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

`IndexMetrics` is a **case class** implemented to retrieve information about your table's **OTree index**.

You can use it to **compare the index** build **with different indexing parameters** such as the `desiredCubeSize` and `columnsToIndex`.

This is meant to be used as an easy access point to analyze the resulting index, which should come handy for comparing different index parameters or even implementations.

```scala
val metrics = qbeastTable.getIndexMetrics()

println(metrics)

// EXAMPLE OUTPUT

Tree Index Metrics:
dimensionCount: 3
elementCount: 2879966589
depth: 7
cubeCount: 22217
desiredCubeSize: 500000
avgFan0ut: 8.0
depthOnBalance: 1.4740213633300192

Non-Leaf Cube Size Stats
Quantiles:
- min: 482642
- 1stQ: 542859
- 2ndQ: 557161
- 3rdQ: 576939
- max: 633266
- dev(l1, l2): (0.11743615196254953, 0.0023669553335121983)

(level, average weight, average cube size):
(0, (1.6781478192839184E-4,482642))
(1, (0.001726577786432248,550513))
(2, (0.014704148241220776,566831))
(3, (0.1260420146029599,570841))
(4, (0.7243052757165773, 557425))
(5, (0.4040913470739245,527043))
(6, (0.8873759316622165, 513460))
```

## Metrics
### 1. General index metadata:

- **dimensionCount**: the number of dimensions (indexed columns) in the index.
- **elementCount**: the number of rows in the table.
- **desiredCubeSize**: the desired cube size chosen at the moment of indexing.
- **Number of cubes**: the number of nodes in the index tree.
- **depth**: the number of levels in the tree.
- **avgFanOut**: the average number of children per non-leaf cube. The max value for this metrics is `2 ^ dimensionCount`.
- **depthOnBalance**: how far the depth of the tree is to the theoretical value if we were to have the same number of cubes and max fan out.

### 2. Cube sizes for non-leaf cubes:
`Non-leaf cube size stats` is meant to describe the distribution of inner cube sizes:
- **min**, **max**, **quartiles**, and how far the cube sizes are from the `desiredCubeSize`(**l1 and l2 error**).
- The average normalizedWeight and cube size per level.

### 3. `Map[CubeId, CubeStatus]`
- More information can be extracted from the index tree through `metrics.cubeStatuses`.
