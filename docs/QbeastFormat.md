# Data Lakehouse with Qbeast Format

Based on Delta Lake transaction log protocol, we introduce some changes in order to enable **multi-dimensional indexing** and **efficient sampling** to the current implementation. 
# Lakehouse and Delta log
To address many issues with a two-tier **data lake + warehouse** architecture, open format storage layers such as **Delta Lake** use transaction logs on top of object stores for metadata management and to achieve primarily `ACID` properties and table versioning.

A **transaction log** in Delta Lake holds information about what objects comprise a table and is stored in the same object store and named as `_delta_log/`. Actions such as `Add` and `Remove` are stored in `JSON` or `parquet` files in chronological order and are consulted before any I/O operation to **reconstruct the latest state of the table**. The actual data objects are stored in `parquet` format.

<p align="center">
  <img src="https://raw.githubusercontent.com/Qbeast-io/qbeast-spark/main/docs/images/delta.png" width="600" height="500" />
</p>

Following each write transaction is the creation of a new log file. **Table-level transaction atomicity** is achieved by following the `put-if-absent` protocol for log file naming - only one client can create a log file with a particular name when attempted by multiple users. Each action record in the log has a field `modificationTime` as **timestamp**, which forms the basis for `snapshot isolation` for read transactions. Check [here](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) for more details about Delta Lake logs.

<br/>

# Qbeast-spark on Delta

Qbeast format **extends the Delta Lake Format**, so the data is stored in Parquet
files and the Index Metadata is stored in the Delta log. 

In more details:

* the `schema`, `index columns`, `revisions` and other
  information which is common for all the blocks is stored in the `metaData`
  attribute of the Delta log header.
  ### MetaData of Qbeast on the Delta Log
  ```json
  {
  "metaData": {
    "id": "aa43874a-9688-4d14-8168-e16088641fdb",
    ...
    // THIS IS THE QBEAST METADATA: INCLUDES REVISION ID, 
    // COLUMNS, DESIRED CUBE SIZE, ETC.
    "configuration": {
      "qbeast.lastRevisionID": "1",
      "qbeast.revision.1": "{\"revisionID\":1,\"timestamp\":1637851757680,\"tableID\":\"/tmp/qb-testing1584592925006274975\",\"desiredCubeSize\":10000,\"columnTransformers\":..}"
    },
    "createdTime": 1637851765848
  }
  ```
* the information about individual blocks of indexed data is stored in the `tags`
  attribute of the `add` entries of the Delta log.
  ### AddFile of Qbeast on the Delta Log
  ```json
  // ALL INFO IN THE TAGS ATTRIBUTE
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
  ]"}
  ```

Let's take a look to the Index metadata to understand what is written in the Delta Log JSON files.

## Index Metadata

At a high level:

- the index consists of one or more `Otree` instances (search trees) that contain `cubes` (or nodes). 
- Each cube is made of `blocks` that contain the actual data written by the user. 

>All records from the log are of **file-level** information.


## AddFile changes

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

| Term           | Description                                                                                                                                                 |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `revision`     | A revision establishes the min-max range for each indexed column, amongst other metadata (creation timestamp, cube size...).                                |
| `blocks`       | A **list of the blocks** that are stored inside the file. Can belong to different cubes.*                                                                   |
| `cube`         | The **String representation of the Cube ID**.                                                                                                               |
| `minWeight`    | The minimum weight of the block.                                                                                                                            |
| `maxWeight`    | The maximum weight of the block.                                                                                                                            |
| `elementCount` | The number of elements in the block.                                                                                                                        |
| `replicated`   | A flag that indicates **if the block is replicated**. Replication is [**not available** from v0.6.0](https://github.com/Qbeast-io/qbeast-spark/issues/282). |

> *Blocks are not directly addressed from the file reader. Check issue [#322](https://github.com/Qbeast-io/qbeast-spark/issues/322) for more info

## MetaData changes

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


| Term                    | Description                                                                                                     |
|-------------------------|-----------------------------------------------------------------------------------------------------------------|
| `qbeast.lastRevisionID` | A pointer to the last revision available. This last revision's min-max includes the space for the whole dataset. |
| `qb.revision.$number`   | The metadata of the Revision, in `JSON` style.                                                                    |

## Revision

A closer look to the Revision Schema:

```json

{
  "revisionID":1, // Incremental ID
  "timestamp":1637851757680, // Time of creation
  "tableID":"/tmp/qb-testing1584592925006274975", // Table ID
  "desiredCubeSize":10000, // Desired cube size, in rows
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

| Term                            | Description                                                                                                                                           |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `revisionID`                    | The ID of the Revision. Used on the AddFile tags to identify the revision of a particular file.                                                       |
| `timestamp`                     | The time when the revision was created.                                                                                                               |
| `tableID`                       | The identifier of the table that the revision belongs to.                                                                                             |
| `desiredCubeSize`               | The cube size from the option `cubeSize`.                                                                                                             |
| `columnTransformers`            | The metadata of the different columns indexed with the option `columnsToIndex`.                                                                       |
| `columnTransformers.columnName` | The **name of the column**.                                                                                                                           |
| `columnTransformers.dataType`   | The **data type of the column**.                                                                                                                      |
| `transformations`               | Classes to **map original values of columns indexed into the [0.0, 1.9) space**. Contains the `min` and `max` values for each column of the Revision. |
| `transformations.className`     | The type of transformation (could be `Linear`, `Hash`...).                                                                                            |
| `transformations.minNumber`     | The **minimum value of the data in the revision**.                                                                                                    |
| `transformations.maxNumber`     | The **maximum value of the data in the revision**.                                                                                                    |
| `transformations.nullValue`     | The **value that represents the `null`** in the space.                                                                                                |


In this case, we index columns `user_id` and `product_id` (which are both `Integers`) with a linear transformation. This means that they will not suffer any transformation besides the normalization.

### Staging Revision and ConvertToQbeastCommand
The introduction of **the Staging Revision enables reading and writing tables in a hybrid `qbeast + delta` state.**
The non-qbeast `AddFile`s are considered as part of this staging revision, all belonging to the root.

Its **RevisionID** is fixed to `stagingID = 0`, and it has `EmptyTransformer`s and `EmptyTransformation`s.
It is **automatically created during the first write or when overwriting a table using `qbeast`**.
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

> Conversion on a partitioned table is not supported.


## Optimization

**Optimize** is an expensive operation that consist of:
- moving offsets to their descendent cubes (files).
- rolling-up small cubes (files) into their parents, creating new files with a size closer to the `desiredCubeSize`.

Thus accomplishing a **better file layout and query performance**.

To minimize write amplification of this command, **we execute it based on subsets of the table**, like `Revision ID's` or specific files.

`Revisions` determine the area of the index. **The last Revision would contain the whole dataset area**, while previous Revisions would contain a _subset_ of the area written before.
It can be useful to optimize by specific Revision ID's when we have an incremental appends scenario.

As an example, imagine you have a table called `city_taxi` that contains data about location of taxi stations of one or more countries.

An initial commit C1 is inserting data into the table about taxis in France, assuming a column called `city_name` indexed with Qbeast.
With this first commit C1, we add three files F1, F2 and F3.

> The first commit C1 creates a revision R1. The space contained on revision R1 is `[city_name_min = France, city_name_max = France]`

A second commit `C2` adds data about German cities, so it expands the cardinality of the `city_name` column.
This commit adds another four files F4, F5, F6 and F7.

>Now, C2 creates revision R2. The space of the revision R2 is `[city_name_min = France, city_name_max = Germany]`

Revision `R2` is necessary because it expands the cardinality of the city name dimension, and this expansion impacts the index as the range of values changes.

Optimizing consequently **can be performed over either revision**.

> Running optimize over R1 would rewrite (only rearranging data) files F1, F2 and F3.

### Optimize API
These are the 3 ways of executing the `optimize` operation:

```scala
qbeastTable.optimize() // Optimizes the last Revision Available.
// This does NOT include previous Revision's optimizations.

qbeastTable.optimize(2L) // Optimizes the Revision number 2.

qbeastTable.optimize(Seq("file1", "file2")) // Optimizes the specific files
```

**If you want to optimize the full table, you must loop through `revisions`**:

```scala
val revisions = qbeastTable.revisionsIDs() // Get all the Revision ID's available in the table.
revisions.foreach(revision => 
  qbeastTable.optimize(revision)
)
```
> Note that **Revision ID number 0 is reserved for Stagin Area** (non-indexed files). This ensures compatibility with underlying table formats.


## Index Replication (&lt;v0.6.0)


> Analyze and Replication operations are **NOT available from version 0.6.0**. Read all the reasoning and changes on the [Qbeast Format 0.6.0](./QbeastFormatChanges.md) document.
