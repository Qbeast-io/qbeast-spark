## Columns To Index Selector

Qbeast Format organizes the records using a multidimensional index. This index is built on a subset of the columns in the table. From `0.6.0` version, **the columns can be selected automatically by enabling the automatic column index selector or manually by the user**.

If you want to forget about the distribution and let qbeast handle all the indexing pre-process, there's no need to specify the `columnsToIndex` in the **DataFrame**.

You only need to **enable the Columns To Index Selector in the `SparkConf`**:

```shell
--conf spark.qbeast.index.columnsToIndex.auto=true \
--conf spark.qbeast.index.columnsToIndex.auto.max=10
```

And **write the DataFrame as usual**:

```scala
df.write.format("qbeast").save("path/to/table")
```

Or use SQL:

```scala
spark.sql("CREATE TABLE table_name USING qbeast LOCATION 'path/to/table'")
```
### Interface

The `ColumnsToIndexSelector` is an interface that can be implemented by different classes. The interface is defined as follows:

```scala
trait ColumnsToIndexSelector[DATA] {

  /**
   * The maximum number of columns to index.
   * @return
   */
  def MAX_COLUMNS_TO_INDEX: Int

  /**
   * Selects the columns to index given a DataFrame
   * @param data
   *   the data to index
   * @return
   */
  def selectColumnsToIndex(data: DATA): Seq[String] =
    selectColumnsToIndex(data, MAX_COLUMNS_TO_INDEX)

  /**
   * Selects the columns to index with a given number of columns to index
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index
   * @return
   *   A sequence with the names of the columns to index
   */
  def selectColumnsToIndex(data: DATA, numColumnsToIndex: Int): Seq[String]

}

```

### SparkColumnsToIndexSelector

`SparkColumnsToIndexSelector` is the first implementation of the `ColumnsToIndexSelector` process. Is designed to work with Apache Spark DataFrames and **provides functionality to automatically select columns for indexing based on certain criteria**.

The steps are the following:

1. **Convert Timestamp columns** to Unix timestamps and update the DataFrame.
2. **Initialize Vector Assembler** for each column. For String columns, transform them into numeric with StringIndexer.
4. **Combine features** from VectorAssembler into a Single Vector column.
5. Calculate the **Correlation Matrix**.
6. Calculate the **absolute correlation** for each column.
7. Get the **top N columns that have the lowest average correlation**.