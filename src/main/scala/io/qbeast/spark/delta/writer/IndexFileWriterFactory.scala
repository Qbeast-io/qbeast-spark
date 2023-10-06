/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.TableChanges
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Factory for creating index file writers.
 *
 * @param tableId the table identifier
 * @param schema the table schema
 * @param tableChanges the table changes
 * @param writerFactory the output writer factory
 * @param trackers the stats trackers
 * @param config the Hadoop configuration
 */
private[writer] class IndexFileWriterFactory(
    tableId: QTableID,
    schema: StructType,
    tableChanges: TableChanges,
    writerFactory: OutputWriterFactory,
    trackers: Seq[WriteJobStatsTracker],
    config: SerializableConfiguration)
    extends Serializable {

  private val generatorFactory = new IndexFileGeneratorFactory(
    tableId,
    schema,
    tableChanges.updatedRevision.revisionID,
    writerFactory,
    config)

  /**
   * Creates a new index file writer.
   *
   * @return the new index file writer
   */
  def createIndexFileWriter(): IndexFileWriter = {
    val generator = generatorFactory.createIndexFileGenerator()
    val taskTrackers = trackers.map(_.newTaskInstance())
    val target = new DefaultIndexFileWriter(generator, taskTrackers, tableChanges)
    val limit = tableChanges.updatedRevision.desiredCubeSize / 10
    new BufferedIndexFileWriter(target, limit)
  }

}
