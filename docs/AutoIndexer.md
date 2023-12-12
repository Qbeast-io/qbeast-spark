SparkAutoIndexer

SparkAutoIndexer is designed to work with Apache Spark DataFrames and provides functionality to automatically select columns for indexing based on certain criteria.

    chooseColumnsToIndex(data: DataFrame): Seq[String]
        Description: Chooses columns to index from the given DataFrame without limiting the number of columns.
        Parameters:
            data: DataFrame - The DataFrame from which columns are selected for indexing.
        Returns: Seq[String] - A sequence of column names selected for indexing.

    chooseColumnsToIndex(data: DataFrame, numColumnsToIndex: Int): Seq[String]
        Description: Chooses a specified number of columns to index from the given DataFrame.
        Parameters:
            data: DataFrame - The DataFrame from which columns are selected for indexing.
            numColumnsToIndex: Int - The number of columns to index.
        Returns: Seq[String] - A sequence of column names selected for indexing.