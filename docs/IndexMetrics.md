# Peeking into the OTree Index Metrics

The API is implemented to retrieve information about your table's **OTree index**.

You can use it to compare the index build with different indexing parameters such as the `desiredCubeSize` and `columnsToIndex`.

If you're experimenting with new ways of implementing the OTree algorithm, you can also use this API to analyze the resulting index!

## Metrics
1. General index metadata:
- **Desired cube size**
- **Number of cubes**
- **Tree depth**
- **Average fan out of the cubes**
- **Dimension count**
- **Number of rows**

2. Some more specific details such as:
- **depthOverLogNumNodes = depth / log(cubeCounts)**
- **depthOnBalance = depth / log(rowCount/desiredCubeSize)**

  both logs use **base = dimensionCount**

3. Cube sizes for non-leaf cubes:
- `NonLeafCubeSizeDetails` contains their **min**, **max**, **quantiles**, and how far each of the cube sizes are from the `desiredCubeSize`(**dev**).

4. `Map[CubeId, CubeStatus]`
- Some information from the map can be interesting to analyze - for example, the distribution of cube weights. You can access this information through `metrics.cubeStatuses`.


## How to use it
```scala
import io.qbeast.spark.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, tablePath)
val metrics = qbeastTable.getIndexMetrics()

println(metrics)
```

## Example output:

```
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
