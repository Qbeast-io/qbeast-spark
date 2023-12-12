## Auto Indexer

Qbeast Format organizes the records using a multidimensional index. This index is built on a subset of the columns in the table. From `1.0.0` version, **the columns can be selected automatically by the Auto Indexer or manually by the user**.

If you want to forget about the distribution and let qbeast handle all the indexing pre-process, there's no need to specify the `columnsToIndex` in the **DataFrame**.

You only need to **enable the Auto Indexer in the `SparkConf`**:

```shell
--conf spark.qbeast.index.autoIndexerEnabled=true \
--conf spark.qbeast.index.maxColumnsToIndex=10
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

The Auto Indexer is an interface that can be implemented by different classes. The interface is defined as follows:

```scala
trait AutoIndexer[DATA] {

  /**
   * The maximum number of columns to index.
   * @return
   */
  def MAX_COLUMNS_TO_INDEX: Int

  /**
   * Chooses the columns to index without limiting the number of columns.
   * @param data
   *   the data to index
   * @return
   */
  def chooseColumnsToIndex(data: DATA): Seq[String]

  /**
   * Chooses the columns to index.
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index if not specified, the default value is
   *   MAX_NUM_COLUMNS_TO_INDEX
   * @return
   *   A sequence with the names of the columns to index
   */
  def chooseColumnsToIndex(data: DATA, numColumnsToIndex: Int): Seq[String]

}

```

### SparkAutoIndexer

`SparkAutoIndexer` is the first implementation of the AutoIndexer process. Is designed to work with Apache Spark DataFrames and **provides functionality to automatically select columns for indexing based on certain criteria**.

The steps are the following:

1. **Convert Timestamp columns** to Unix timestamps and update the DataFrame.
2. **Initialize Vector Assembler** for each column. For String columns, transform them into numeric with StringIndexer.
4. **Combine features** from VectorAssembler into a Single Vector column.
5. Calculate the **Correlation Matrix**.
6. Calculate the **absolute correlation** for each column.
7. Get the **top N columns that have the lowest average correlation**.