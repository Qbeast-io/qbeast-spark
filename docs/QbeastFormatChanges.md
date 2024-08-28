# Qbeast Format Changes 

**The version 0.6.0 introduces a new format for the Qbeast tables.** 

This document states the main changes and how we can recover a table written with a past version.

# Metadata changes

* **The information about the replicated cubes are not stored in the Delta log
  anymore.**
* **A physical file can contain data from multiple `blocks`**, so the information
  stored in the `tags` of an `add`entry in the Delta log provides information
  about all the blocks stored in the corresponding index file.

## Block metadata before v0.6.0

Before the version 0.6.0 the `tags` attribute of the `add` entry contained a JSON
like the following:

```JSON
"tags": {
  "state": "FLOODED",
  "cube": "w",
  "revision": "1",
  "minWeight": "2",
  "maxWeight": "3",
  "elementCount": "4" 
}
```


| Term            | Description                                    |
|-----------------|------------------------------------------------|
| `cube`          | The serialized representation of the Cube.     |
| `revision`      | The metadata of the tree.                      |
| `elementCount`  | The number of elements in the block.           |
| `minWeight`     | The minimum weight of the block.               |
| `maxWeight`     | The maximum weight of the block.               |

## Block metadata from v0.6.0

From the version 0.6.0 the `tags` attribute of the add entry contains a JSON
like the following:

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
The `blocks` attribute contains a string which is a JSON with necessary escapes
for the quotes.


# How to transform an existing table

It is possible to transform a table written with the library with the version
before 0.6.0 into a table compatible with the version 0.6.0 without reindexing
the table data. The utility should implement the following steps:

* remove the information about the replicated cubes from the Delta log metadata
* for each `add` entry in the Delta log remove all the tags except the 
  `revision` tag and add the `blocks` tag containing the only block having the
  same values for the `cube`, `minWeight`, etc attributes. The `replicated`
  attribute of the block should be `true` if and only if the value of the
  `state` tag in the original entry is `REPLICATED` or `ANNOUNCED`

To automate the transformation it is strongly recommended to use an
  appropriate Delta library which allows to manipulate the Delta log, for
  example, in case of Scala use `"io.delta" %% "delta-core" % 2.4.0` or above.
