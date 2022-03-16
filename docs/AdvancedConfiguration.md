# Advanced configurations

There's different configurations for the index that can affect the performance on read or the writing process. Here is a resume of some of them.

## ColumnsToIndex

These are the columns you want to index. Try to find those which are interesting for your queries, or your data pipelines. 

You can specify different advanced options to the columns to index:

- **Type**: The type of the index you want to create in that column. Can be linear (numeric) or hash (string). By default, it would use the type of data.


```scala
df.write.format("qbeast")
  .option("columnsToIndex", "column:type,column2:type...")
```

## CubeSize

CubeSize option lets you specify the maximum size of the cube, in number of records. By default, it's set to 5M.

```scala
df.write.format("qbeast")
  .option("cubeSize", "10000")
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