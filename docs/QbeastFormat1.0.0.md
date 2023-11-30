# QbeastFormat 1.0.0

## What is Qbeast format?

Qbeast format is used to store the big data tables making it efficient querying
and sampling. It extends the Delta format, so the data is stored in parquet
files and the index metadata is stored in the Delta log. In more details

* the schema, index columns, data transformations, revisions and other
  information which is common for all the blocks is stored in the `metaData`
  attribute of the Delta log header.
* the information about individual blocks of index data is stored in the `tags`
  attribute of the `add` entries of the Delta log.

## Metadata changes introduced in the version 1.0.0

* The information about the replicated cubes are not stored in the Delta log
  anymore
* A physical file can contain data from multiple blocks, so the information
  stored in the `tags` of an `add`entry in the Delta log provides information
  about all the blocks stored in the corresponding index file.

## Block metadata before the version 1.0.0

Before the vesion 1.0.0 the `tags` attribute of the `add` entry contained a JSON
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

where

* possible values for the state are FLOODED, ANNOUNCED and REPLICATED
* cube value is the string representation of the cube identifier
* revision value is the revision identifier
* minWeight/maxWeight define the range of the element weights
* elementCount provides the number of elements in the block 

## Block metadata from the version 1.0.0

From the version 1.0.0 the `tags` attribute of the add entry contains a JSON
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

## How to transform an existing table

It is possible to transform a table written with the library with the version
before 1.0.0 into a table compatible with the version 1.0.0 without reindexing
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
