# QbeastTable

This API is implemented to interact with your QbeastTable.

Creating an instance of QbeastTable is as easy as:

```scala
import io.qbeast.spark.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, "path/to/qbeast/table")
```
## Format Information

If you want to know more about the Format, you can use the different get methods. 

>:information_source: **Note: In each of them you can specify the particular `RevisionID` you want to get the information from.**

```scala
qbeastTable.indexedColumns() // current indexed columns

qbeastTable.cubeSize() // current cube size

qbeastTable.revisionsIDs() // all the current Revision identifiers

qbeatsTable.lastRevisionID() // the last Revision identifier
```

And also execute `Analyze` and `Optimize` operations, which are **currently experimental**.

```scala
qbeastTable.analyze() // returns the Serialized cube ID's to optimize

qbeastTable.optimize() // optimizes the cubes
```

## Index Metrics

`IndexMetrics` is a **case class** implemented to retrieve information about your table's **OTree index**.

You can use it to **compare the index** build **with different indexing parameters** such as the `desiredCubeSize` and `columnsToIndex`.

If you're experimenting with new ways of implementing the OTree algorithm, you can also use this API to analyze the resulting index!

```scala
val metrics = qbeastTable.getIndexMetrics()

println(metrics)

// EXAMPLE OUTPUT

OTree Index Metrics:
dimensionCount: 2
elementCount: 1001
depth: 2
cubeCounts: 7
desiredCubeSize: 100
avgFanOut: 2.0
depthOverLogNumNodes: 0.7124143742160444
depthOnBalance: 0.6020599913279624
Non-lead Cube Size Stats:
(All values are 0 if there's no non-leaf cubes):
- min: 3729
- firstQuartile: 3729
- secondQuartile: 4832
- thirdQuartile: 5084
- max: 5084
- dev: 2.0133907E7
```

## Metrics
### 1. General index metadata:

- **Desired cube size**: the desired cube size choosed by the user, or the one that was automatically calculated.
- **Number of cubes**: the number of cubes in the index.
- **Tree depth**: the number of levels of the tree.
- **Average fan out of the cubes**: the average number of children per non-leaf cube. For this metric, it is better to get closer to `2^(numberOfDimensions)`.
- **Dimension count**: the number of dimensions (indexed columns) in the index.
- **Number of rows**: the total number of elements.

### 2. Some more specific details such as:
- **depthOverLogNumNodes = depth / log(cubeCounts)**
- **depthOnBalance = depth / log(rowCount/desiredCubeSize)**

  both logs use **base = dimensionCount**

### 3. Cube sizes for non-leaf cubes:
- `NonLeafCubeSizeDetails` contains their **min**, **max**, **quantiles**, and how far each of the cube sizes are from the `desiredCubeSize`(**dev**).

### 4. `Map[CubeId, CubeStatus]`
- Some information from the map can be interesting to analyze - for example, the **distribution of cube weights**.

  You can access this information through `metrics.cubeStatuses`.
