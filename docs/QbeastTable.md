# QbeastTable

This API is implemented to interact with your QbeastTable.

Creating an instance of QbeastTable is as easy as:

```scala
import io.qbeast.table.QbeastTable

val qbeastTable = QbeastTable.forPath(spark, "path/to/qbeast/table")
```
## Table Information

If you want to know more about the Format, you can use the different get methods. 

>:information_source: **Note: In each of them you can specify the particular `RevisionID` you want to get the information from.**

```scala
qbeastTable.indexedColumns() // current indexed columns

qbeastTable.cubeSize() // current cube size

qbeastTable.allRevisionIDs() // all the current Revision identifiers

qbeatsTable.lastRevisionID() // the last Revision identifier
```

## Table Operations
Through `QbeastTable` you can also execute the `Optimize` operation. This command is used to **rearrange the files in the table** according to the dictates of the index to render queries more efficient. 

These are a few different ways of executing the `optimize` operation, with the input parameters being:
1. `revisionID`: The revision number you want to optimize, the default is the latest revision.
2. `fraction`: The fraction of the data of the specified revision you want to optimize.
3. `options`: A map of options of the optimization. You can specify the `userMetadata` as well as configurations for `io.qbeast.spark.delta.hook.PreCommitHook.`

```scala
// Optimizing 10% of the data from Revision number 2, and stores some user metadata
qbeastTable.optmize(2L, 0.1, Map["userMetadata" -> "user-metadata-for-optimization"])

// Optimizing all data from a given Revision
qbeastTable.optimize(2L)

// Optimizing the latest available Revision.
qbeastTable.optimize()

// Optimizes the specific files
qbeastTable.optimize(Seq("file1", "file2"))
```

## Index Metrics

`IndexMetrics` provides an overview of a given revision of the index.

You can use it during development to compare indexes built using different indexing parameters such as the `desiredCubeSize` and `columnsToIndex.`


```scala
val metrics = qbeastTable.getIndexMetrics()

println(metrics)
```

```
// EXAMPLE OUTPUT
OTree Index Metrics:
revisionId: 1
elementCount: 309008
dimensionCount: 2
desiredCubeSize: 3000
indexingColumns: price:linear,user_id:linear
height: 8 (4)
avgFanout: 3.94 (4.0)
cubeCount: 230
blockCount: 848
fileCount: 61
bytes: 11605372

Multi-block files stats:
cubeElementCountStats: (count: 230, avg: 1343, std: 1186, quartiles: (1,292,858,2966,3215))
blockElementCountStats: (count: 848, avg: 364, std: 829, quartiles: (1,6,21,42,3120))
fileBytesStats: (count: 61, avg: 190252, std: 57583, quartiles: (113168,139261,182180,215136,332851))
blockCountPerCubeStats: (count: 230, avg: 3, std: 1, quartiles: (1,4,4,4,4))
blockCountPerFileStats: (count: 61, avg: 13, std: 43, quartiles: (1,3,5,5,207))

Inner cubes depth-wise stats:
cubeElementCountStats: (count: 58, avg: 3069, std: 58, quartiles: (2942,3026,3070,3109,3215))
blockElementCountStats: (count: 232, avg: 767, std: 1278, quartiles: (14,26,33,2859,3120))
depth avgCubeElementCount cubeCount blockCount cubeElementCountStd cubeElementCountQuartiles  avgWeight           
0     3026                1         4          0                   (3026,3026,3026,3026,3026) 0.009708486732493268
1     3120                2         8          72                  (3048,3048,3193,3193,3193) 0.1777036037942636  
2     3088                3         12         91                  (3003,3003,3048,3215,3215) 0.29339823296605566 
3     3078                7         28         45                  (3016,3046,3070,3110,3168) 0.3028351948969015  
4     3075                12        48         80                  (2942,3027,3084,3148,3214) 0.3853194593029685  
5     3056                21        84         45                  (2975,3016,3066,3090,3127) 0.557916611995329   
6     3071                12        48         37                  (2999,3055,3084,3111,3115) 0.71568557500894    

Leaf cubes depth-wise stats:
cubeElementCountStats: (count: 172, avg: 761, std: 733, quartiles: (1,163,584,1159,2966))
blockElementCountStats: (count: 616, avg: 212, std: 498, quartiles: (1,4,10,36,2877))
depth avgCubeElementCount cubeCount blockCount cubeElementCountStd cubeElementCountQuartiles avgWeight         
1     47                  2         4          46                  (1,1,94,94,94)            1515.9574468085107
2     358                 4         12         389                 (4,36,424,970,970)        210.8753971341503 
3     1010                5         20         994                 (314,330,677,767,2966)    5.5998339972396405
4     836                 14        45         951                 (4,38,315,1612,2552)      106.06026824809581
5     978                 27        103        827                 (63,299,616,1681,2666)    8.58027316980015  
6     811                 72        267        686                 (7,280,626,1253,2889)     21.17595889195726 
7     580                 48        165        590                 (3,143,515,779,2246)      76.10800345961682  
```

## Metrics
### 1. General index metadata:


- **revisionId**: the identifier for the index revision
- **elementCount**: the number of records for this revision
- **dimensionCount**: the number of dimensions in the index
- **desiredCubeSize**: the target number of elements per cube
- **indexingColumns**: the columns that are indexed. The format is `column1:type,column2:type,...` where `type` is the transformation type applied to the column
- **height**: the height of the tree. In parentheses, we have the theoretical height if the index is perfectly balanced
- **avgFanOut**: the average number of children per non-leaf cube. In parentheses, we have the max value for this metric, which is equal to `2 ^ dimensionCount`
- **cubeCount**: the total number of nodes in the index tree
- **blockCount**: the number of blocks in the index tree
- **fileCount**: the number of files in the index tree
- **bytes**: the total size of the index tree in bytes

### 2. Multi-block files stats::
The following metrics describe the distribution of the cubes, blocks, and files in the index tree:
- **cubeElementCountStats**: the statistics of the number of elements per cube
- **blockElementCountStats**: the statistics of the number of elements per block
- **fileBytesStats**: the statistics of the size of the files
- **blockCountPerCubeStats**: the statistics of the number of blocks per cube
- **blockCountPerFileStats**: the statistics of the number of blocks per file

For example, `cubeElementCountStats: (count: 230, avg: 1343, std: 1186, quartiles: (1,292,858,2966,3215))` means that
- there are`230` cubes
- the average number of elements per cube is `1343`
- the standard deviation is `1186`
- the quartiles are `(min:1, 1stQ:292, 2ndQ:858, 3rdQ:2966, max:3215)`

### 3. Depth-wise cube statistics:
The following metrics describe the distribution of the cubes in the index tree based on their depth, specifically for the inner cubes:
```bash
Inner cubes depth-wise stats:
cubeElementCountStats: (count: 58, avg: 3069, std: 58, quartiles: (2942,3026,3070,3109,3215))
blockElementCountStats: (count: 232, avg: 767, std: 1278, quartiles: (14,26,33,2859,3120))
depth avgCubeElementCount cubeCount blockCount cubeElementCountStd cubeElementCountQuartiles  avgWeight           
0     3026                1         4          0                   (3026,3026,3026,3026,3026) 0.009708486732493268
1     3120                2         8          72                  (3048,3048,3193,3193,3193) 0.1777036037942636  
2     3088                3         12         91                  (3003,3003,3048,3215,3215) 0.29339823296605566 
3     3078                7         28         45                  (3016,3046,3070,3110,3168) 0.3028351948969015  
4     3075                12        48         80                  (2942,3027,3084,3148,3214) 0.3853194593029685  
5     3056                21        84         45                  (2975,3016,3066,3090,3127) 0.557916611995329   
6     3071                12        48         37                  (2999,3055,3084,3111,3115) 0.71568557500894    
```

`cubeElementCountStats` and `blockElementCountStats` are the same as in the previous section. The same metrics are displayed for each level of the index tree.
