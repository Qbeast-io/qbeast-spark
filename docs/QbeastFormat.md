# Data Lakehouse with Qbeast Format

Based on Delta Lake transaction log protocol, we introduce some changes in order to enable **multi-dimensional indexing** and **efficient sampling** to the current implementation. 
## Lakehouse and Delta log
To address many issues with a two-tier **data lake + warehouse** architecture, open format storage layers such as **Delta Lake** use transaction logs on top of object stores for metadata management and to achieve primarily `ACID` properties and table versioning.

A **transaction log** in Delta Lake holds information about what objects comprise a table and is stored in the same object store and named as `_delta_log/`. Actions such as `Add` and `Remove` are stored in `JSON` or `parquet` files in chronological order and are consulted before any I/O operation to **reconstruct the latest state of the table**. The actual data objects are stored in `parquet` format.


<p align="center">
  <img src="./images/delta.png" width=600 height=500>
</p>


Following each write transaction is the creation of a new log file. **Table-level transaction atomicity** is achieved by following the `put-if-absent` protocol for log file naming - only one client can create a log file with a particular name when attempted by multiple users. Each action record in the log has a field `modificationTime` as **timestamp**, which forms the basis for `snapshot isolation` for read transactions. Check [here](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) for more details about Delta Lake logs.

<br/>

## Qbeast-spark on Delta

Qbeast format extends the Delta Lake Format, so the data is stored in Parquet
files and the Index Metadata is stored in the Delta log. 

In more details:

* the schema, index columns, data transformations, revisions and other
  information which is common for all the blocks is stored in the `metaData`
  attribute of the Delta log header.
* the information about individual blocks of index data is stored in the `tags`
  attribute of the `add` entries of the Delta log.

Let's take a look to the Index metadata to understand what is written in the Delta Log JSON files.

### Index Metadata

On a high level, the index consists of one or more `OTrees` that contain `cubes` (or nodes), and each cube is made of `blocks` that contain the actual data written by the user.
All records from the log are of **block/file-level** information.

| Term              | Description                                                                                                                                                                                                                                                                                               |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `revision`        | Locates the particular tree in the space, which includes the min/max values for each column indexed and timestamp.                                                                                                                                                                                |
| `transformations` | Inside `revision` consist of two maps (in this case), each corresponding to one of the `indexedColumns`. Each pair of `min`/`max` defines the range of values of the associated indexed column that the `tree` can contain and is to be expanded to accommodate new rows that fall outside the current range. |
| `cube`            | Identifies the current `block`'s `cube` from the `tree`.                                                                                                                                                                                                                                                  |
| `state`           | Of the `block` determines the **READ** and **WRITE** protocols. It can be `FLOODED`, `ANNOUNCED`, or `REPLICATED`.                                                                                                                                                                                        |
| `weightMax/weightMin` | Each element gets assigned with a uniformly created `weight` parameter. `weightMin` and `weightMax` define the range of weights that the `block` can contain.                                                                                                                                             |


### AddFile changes

Here you can see the changes on the `AddFile` **`tags`** information

```JSON
"tags": {
  "revision": "1",
  "blocks": "[
    {
      \"cube\": \"w\",
      \"minWeight\": 2,
      \"maxWeight\": 3,
      \"replicated\": false,
      \"elementCount\": 4
    },
    {
      \"cube\": \"wg\",
      \"minWeight\": 5,
      \"maxWeight\": 6,
      \"replicated\": false,
      \"elementCount\": 7
    },
  ]" 
}
```

| Term           | Description                                                                          |
|----------------|--------------------------------------------------------------------------------------|
| `revision`     | The metadata of the tree.                                                            |
| `blocks`       | A list of the blocks that are stored inside the file. Can belong to different cubes. |
| `cube`         | The serialized representation of the Cube.                                           |
| `elementCount` | The number of elements in the block.                                                 |
| `minWeight`    | The minimum weight of the block.                                                     |
| `maxWeight`    | The maximum weight of the block.                                                     |



### MetaData changes

And here the changes on `Metadata` `configuration` map

```json
{
  "metaData": {
    "id": "aa43874a-9688-4d14-8168-e16088641fdb",
    ...
    "configuration": {
      "qbeast.lastRevisionID": "1",
      "qbeast.revision.1": "{\"revisionID\":1,\"timestamp\":1637851757680,\"tableID\":\"/tmp/qb-testing1584592925006274975\",\"desiredCubeSize\":10000,\"columnTransformers\":..}"
    },
    "createdTime": 1637851765848
  }
}

```
We store two different values:


| Term                    | Description                                         |
|-------------------------|-----------------------------------------------------|
| `qbeast.lastRevisionID` | A pointer to the last revision available.           |
| `qb.revision.$number`   | The characteristics of the Revision, in JSON style. |

### Revision

A more closer look to the `qb.revision.$number`:

```json

{
  "revisionID":1,
  "timestamp":1637851757680,
  "tableID":"/tmp/qb-testing1584592925006274975",
  "desiredCubeSize":10000,
  "columnTransformers":[
    {
      "className":"io.qbeast.core.transform.LinearTransformer",
      "columnName":"user_id",
      "dataType":"IntegerDataType"
    },
    {
      "className":"io.qbeast.core.transform.LinearTransformer",
      "columnName":"product_id",
      "dataType":"IntegerDataType"
    }
  ],
  "transformations":[
    {
      "className":"io.qbeast.core.transform.LinearTransformation",
      "minNumber":315309190,
      "maxNumber":566280860,
      "nullValue":476392009,
      "orderedDataType":"IntegerDataType"
    },
    {
      "className":"io.qbeast.core.transform.LinearTransformation",
      "minNumber":1000978,
      "maxNumber":60500010,
      "nullValue":6437856,
      "orderedDataType":"IntegerDataType"}
  ]
}

```

In Revision, you can find different information about the tree status and configuration:

| Term                            | Description                                                                                     |
|---------------------------------|-------------------------------------------------------------------------------------------------|
| `timestamp`                     | The time when the revision was created.                                                         |
| `tableID`                       | The identifier of the table that the revision belongs to.                                       |
| `desiredCubeSize`               | The cube size from the option `cubeSize`.                                                       |
| `columnTransformers`            | The metadata of the different columns indexed with the option `columnsToIndex`.                 |
| `columnTransformers.columnName` | The name of the column.                                                                         |
| `columnTransformers.dataType`   | The data type of the column.                                                                    |
| `transformations`               | Contains information about the **space** of the data indexed by column.                         |
| `transformations.className`     | The name of the class that implements the transformation.                                       |
| `transformations.minNumber`     | The minimum value.                                                                              |
| `transformations.maxNumber`     | The maximum value.                                                                              |
| `transformations.nullValue`     | The value that represents the null in the space.                                                |


In this case, we index columns `user_id` and `product_id` (which are both `Integers`) with a linear transformation. This means that they will not suffer any transformation besides the normalization.

### Staging Revision and ConvertToQbeastCommand
The introduction of the staging revision enables reading tables in a hybrid `qbeast + delta` state.
The non-qbeast `AddFile`s are considered as part of this staging revision, all belonging to the root.

Its RevisionID is fixed to `stagingID = 0`, and it has `EmptyTransformer`s and `EmptyTransformation`s.
It is automatically created during the first write or when overwriting a table using qbeast.
For a table that is entirely written in `delta` or `parquet`, we can use the `ConvertToQbeastCommand` to create this revision:
```scala
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand

val path = "/pathToTable/"
val tableIdentifier = s"parquet.`$path`"
val columnsToIndex = Seq("col1", "col2", "col3")
val desiredCubeSize = 50000

ConvertToQbeastCommand(tableIdentifier, columnsToIndex, desiredCubeSize).run(spark)

val qTable = spark.read.format("qbeast").load(path)
```
By doing so, we also enable subsequent appends using either delta or qbeast.
Conversion on a partitioned table is not supported.

### Compaction

`Compaction` can be performed on the staging revision to group small delta files:
```scala
import io.qbeast.spark.QbeastTable

val table = QbeastTable.forPath(spark, "/pathToTable/")
table.compact(0)
```

### Index Replication


> Analyze and Replication operations are NOT available from version 0.6.0. Read all the reasoning and changes on the [Qbeast Format 0.6.0](./QbeastFormat0.6.0.md) document. 