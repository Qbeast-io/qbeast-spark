package io.qbeast.spark.internal.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}

class QbeastFileScanRDD(
    private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient override val filePartitions: Seq[FilePartition])
    extends FileScanRDD(sparkSession, readFunction, filePartitions) {


}
