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

### Changes on _delta_log/

> We are working on providing a **unified metadata structure** for Qbeast Format.
>
> Any feedback is welcome!

**Qbeast-spark** extends Delta Lake to enhance **Data Lakehouses** with functionalities such as `multi-dimensional indexing`, efficient `sampling`, `table tolerance`, etc., through modifying the log files on the **record level**. Each record's `tags` field has information that describes the `OTree`, `cube`, and `block` involved in the operation.

- Changes on the `AddFile` **`tags`** information
```json
{
  "add": {
    "path": "4b36340e-0bf7-44ec-97da-f7a03bf06ea3.parquet",
    ...
    "tags": {
      "state": "FLOODED",
      "rowCount": "3",
      "cube": "gA",
      "revision": "1634196697656",
      "minWeight": "-2147483648",
      "maxWeight": "-857060062"
    }
  }
}
```
- Changes on `Metadata` `configuration` map

```json
{"metaData":{
  "id":"a5c2699f-62dd-4750-8384-be3a2caa55c7",
  ...
  "configuration": {
    "qbeast.lastRevisionID":"1634196697656",
    "qbeast.revision.1634196697656":"{\"id\":1634196697656,\"timestamp\":1634196697656,\"desiredCubeSize\":10000,\"indexedColumns\":[\"user_id\",\"product_id\"],\"transformations\":[{\"min\":1.16396325E8,\"max\":7.13218881E8,\"scale\":1.675539890285246E-9},{\"min\":-2.87485335E7,\"max\":9.02495125E7,\"scale\":8.403499331409189E-9}]}"
  },
  "createdTime":1634196701990}}
```
We store two different values:
- A pointer to the last revision available `qbeast.lastRevisionID`
- The different characteristics of this revision (`qb.revision.1634196697656`). 

A more closer look to the `qb.revision.1634196697656`:

```json

{
  "id": 1634196697656,
  "timestamp": 1634196697656,
  "desiredCubeSize": 10000,
  "indexedColumns": ["user_id", "product_id"],
  "transformations": [
    {
      "min": 1.16396325E8,
      "max": 7.13218881E8,
      "scale": 1.675539890285246E-9
    },
    {
      "min": -2.87485335E7,
      "max": 9.02495125E7,
      "scale": 8.403499331409189E-9
    }
  ]
}
```

On a high level, the index consists of one or more `OTrees` that contain `cubes`(or nodes), and each cube is made of `blocks` that contain the actual data written by the user. All records from the log are of **block-level** information.

- `revision` locates the tree that contains the `block`
  

- `cube` identifies the current `block`'s `cube` from the `tree`
  

- `state` of the `block` determines the **READ** and **WRITE** protocols
  

- `weightMax/weightMin`: Each element gets assigned with a uniformly created `weight` parameter. `weightMin` and `weightMax` define the range of weights that the `block` can contain.
  

- `transformations` inside `revision` consist of two maps(in this case), each corresponding to one of the `indexedColumns`. Each pair of `min`/`max` defines the range of values of the associated indexed column that the `tree` can contain and is to be expanded to accommodate new rows that fall outside the current range.

### State changes in Metadata

**Data de-normalization** is a crucial component behind our multi-dimensional index. Instead of storing an index in a separate tree-like data structure, we reorganize the data and their replications in an `OTree`, whose **hierarchical structure** is the actual index.

Aside from modifying `_delta_log/`, we also store the `cube` state changes in the `_qbeast/` folder found in the same table directory. During index optimization, affected `blocks` modify their states, and new `blocks` are added. Users can trigger the optimization process manually through `analyze()` and `optimize()` methods.

See [OTreeAlgorithm](./OTreeAlgorithm.md) or the [research paper](https://upcommons.upc.edu/bitstream/handle/2117/180358/The_OTree_for_IEEE_short_paper.pdf?sequence=1) for more details.

```scala
import io.qbeast.spark.table._

val dir = "path/to/mytable"
val qbeastTable = QbeastTable.forPath(spark, dir)

qbeastTable.analyze()

qbeastTable.optimize()
```

**Cubes** are analyzed, and their **states** are changed according to the relation between their `payload` and the number of elements they contain. `cube` state changes can be viewed reading the `metaData` on the last delta commit log:

```json
{"metaData":{
  "configuration":
  {
    ...
    "qbeast.replicatedSet.1634196697656":"[\"\",\"g\",\"gQ\"]"},
    ...
  }
}

```

In this case, some blocks from cubes `root`, `g` and `gQ` transformed to the state of `REPLICATED`. Corresponding actions such as `Add` and `Remove` are recorded in `_delta_log/`, with `revision` from the log shown here as their `timestamp`.
