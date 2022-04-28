package io.qbeast.spark.index.writer

/*
  Data Schema:
  |-- a: struct (nullable = true)
  |    |-- b: struct (nullable = true)
  |    |    |-- c: long (nullable = true)

Collected Statistics:
  |-- stats: struct (nullable = true)
  |    |-- numRecords: long (nullable = false)
  |    |-- minValues: struct (nullable = false)
  |    |    |-- a: struct (nullable = false)
  |    |    |    |-- b: struct (nullable = false)
  |    |    |    |    |-- c: long (nullable = true)
  |    |-- maxValues: struct (nullable = false)
  |    |    |-- a: struct (nullable = false)
  |    |    |    |-- b: struct (nullable = false)
  |    |    |    |    |-- c: long (nullable = true)
  |    |-- nullCount: struct (nullable = false)
  |    |    |-- a: struct (nullable = false)
  |    |    |    |-- b: struct (nullable = false)
  |    |    |    |    |-- c: long (nullable = true)
 */

case class ColumnStatsCollection(min: Any, max: Any, nullCount: Long)
