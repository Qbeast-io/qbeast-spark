/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.table

import io.qbeast.core.model._
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.RevisionFactory
import io.qbeast.sources.QbeastBaseRelation
import io.qbeast.IISeq
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.COLUMN_SELECTOR_ENABLED
import org.apache.spark.qbeast.config.DEFAULT_NUMBER_OF_RETRIES
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

import java.lang.System.currentTimeMillis
import java.util.ConcurrentModificationException
import scala.collection.mutable

/**
 * Indexed table represents the tabular data storage indexed with the OTree indexing technology.
 */
trait IndexedTable {

  /**
   * Returns whether the table physically exists.
   * @return
   *   the table physically exists.
   */
  def exists: Boolean

  /**
   * Returns the schema of the table, if any
   * @return
   *   the schema
   */
  def schema: StructType

  /**
   * Returns whether the table contains Qbeast metadata
   * @return
   */
  def hasQbeastMetadata: Boolean

  /**
   * Automatically computes columnsToIndex from the provided DataFrame
   * @param data
   *   Dataframe
   * @return
   */
  def selectColumnsToIndex(data: Option[DataFrame]): Seq[String]

  /**
   * Returns the table id which identifies the table.
   *
   * @return
   *   the table id
   */
  def tableID: QTableID

  /**
   * Merge new and index current properties
   * @param properties
   *   the properties you want to merge
   * @return
   */
  def verifyAndUpdateParameters(
      properties: Map[String, String],
      data: Option[DataFrame] = None): Map[String, String]

  /**
   * Saves given data in the table and updates the index. The specified columns are used to define
   * the index when the table is created or overwritten. The append flag defines whether the
   * existing data should be overwritten.
   *
   * @param data
   *   the data to save
   * @param parameters
   *   the parameters to save the data
   * @param append
   *   the data should be appended to the table
   * @return
   *   the base relation to read the saved data
   */
  def save(data: DataFrame, parameters: Map[String, String], append: Boolean): BaseRelation

  /**
   * Loads the table data.
   *
   * @return
   *   the base relation to read the table data
   */
  def load(): BaseRelation

  /**
   * Optimizes a given index revision up to a given fraction.
   *
   * @param revisionID
   *   the identifier of revision to optimize
   * @param fraction
   *   the fraction of the index to optimize; this value should be between (0, 1].
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimize(revisionID: RevisionID, fraction: Double, options: Map[String, String]): Unit

  /**
   * Optimizes the table by optimizing the data stored in the specified index files.
   *
   * @param indexFiles
   *   the index files to optimize
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimizeIndexedFiles(indexFiles: Seq[String], options: Map[String, String]): Unit

  /**
   * Optimizes the table by optimizing the data stored in the specified unindexed files.
   * @param unindexedFiles
   *   the unindexed files to optimize
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimizeUnindexedFiles(unindexedFiles: Seq[String], options: Map[String, String]): Unit
}

/**
 * IndexedTable factory.
 */
trait IndexedTableFactory {

  /**
   * Returns a IndexedTable for given SQLContext and path. It is not guaranteed that the returned
   * table physically exists, use IndexedTable#exists attribute to verify it.
   *
   * @param tableId
   *   the table path
   * @return
   *   the table
   */
  def getIndexedTable(tableId: QTableID): IndexedTable
}

/**
 * Implementation of IndexedTableFactory.
 * @param indexManager
 *   the index manager
 * @param metadataManager
 *   the metadata manager
 * @param dataWriter
 *   the data writer
 * @param revisionFactory
 *   the revision builder
 */
final class IndexedTableFactoryImpl(
    private val indexManager: IndexManager,
    private val metadataManager: MetadataManager,
    private val dataWriter: DataWriter,
    private val revisionFactory: RevisionFactory,
    private val columnSelector: ColumnsToIndexSelector)
    extends IndexedTableFactory {

  override def getIndexedTable(tableID: QTableID): IndexedTable =
    new IndexedTableImpl(
      tableID,
      indexManager,
      metadataManager,
      dataWriter,
      revisionFactory,
      columnSelector)

}

/**
 * Implementation of IndexedTable.
 *
 * @param tableID
 *   the table identifier
 * @param indexManager
 *   the index manager
 * @param metadataManager
 *   the metadata manager
 * @param dataWriter
 *   the data writer
 * @param revisionFactory
 *   the revision factory
 * @param columnSelector
 *   the auto indexer
 */
private[table] class IndexedTableImpl(
    val tableID: QTableID,
    private val indexManager: IndexManager,
    private val metadataManager: MetadataManager,
    private val dataWriter: DataWriter,
    private val revisionFactory: RevisionFactory,
    private val columnSelector: ColumnsToIndexSelector)
    extends IndexedTable
    with StagingUtils
    with Logging {
  private var snapshotCache: Option[QbeastSnapshot] = None

  /**
   * Latest Revision Available
   *
   * @return
   */
  private def latestRevision: Revision = snapshot.loadLatestRevision

  override def exists: Boolean = !snapshot.isInitial

  override def schema: StructType = snapshot.schema

  override def hasQbeastMetadata: Boolean = try {
    snapshot.loadLatestRevision
    true
  } catch {
    case _: Exception => false
  }

  /**
   * Update the input parameters with the latest revision information if available. Otherwise,
   * compute the columnsToIndex from the data.
   * @param parameters
   *   the input parameters
   * @param data
   *   the data
   * @return
   *   the updated parameters
   */
  override def verifyAndUpdateParameters(
      parameters: Map[String, String],
      data: Option[DataFrame]): Map[String, String] = {
    // Check configuration membership using the input map as it may be a CaseInsensitiveMap
    val updatedParameters = mutable.Map[String, String](parameters.toSeq: _*)
    if (!exists) {
      // If the table does not exist. Compute the columnsToIndex from the data if needed.
      // The cubeSize, if not provided, will be set to the default value.
      if (!parameters.contains(COLUMNS_TO_INDEX)) {
        val columnsToIndex = selectColumnsToIndex(data)
        updatedParameters += (COLUMNS_TO_INDEX -> columnsToIndex.mkString(","))
      }
    } else if (hasQbeastMetadata) {
      // If the table exists and has Qbeast metadata. Update the parameters if needed.
      if (!parameters.contains(COLUMNS_TO_INDEX)) {
        val currentIndexingColumns =
          latestRevision.columnTransformers.map(_.columnName).mkString(",")
        updatedParameters += (COLUMNS_TO_INDEX -> currentIndexingColumns)
      }
      if (!parameters.contains(CUBE_SIZE)) {
        val currentCubeSize = latestRevision.desiredCubeSize.toString
        updatedParameters += (CUBE_SIZE -> currentCubeSize)
      }
    } else {
      // If the table exists but does not have Qbeast metadata.
      throw AnalysisExceptionFactory.create(
        "The table exists but does not have Qbeast metadata. " +
          "Please provide the columnsToIndex parameters. " +
          "Or use the ConvertToQbeast command to convert the table to Qbeast.")
    }
    updatedParameters.toMap
  }

  /**
   * Selects the columns to index from the data. This operation requires the data to be available
   * and the autoIndexingEnabled to be enabled.
   * @param data
   *   the data
   * @return
   *   the columns to index
   */
  override def selectColumnsToIndex(data: Option[DataFrame]): Seq[String] = {
    if (COLUMN_SELECTOR_ENABLED) {
      data match {
        case Some(df) => columnSelector.selectColumnsToIndex(df)
        case None =>
          throw AnalysisExceptionFactory.create(
            "No data is available to select columnsToIndex from.")
      }
    } else {
      // IF autoIndexingEnabled is disabled, and no columnsToIndex are specified we should throw an exception
      throw AnalysisExceptionFactory.create(
        "Auto indexing is disabled. " +
          """You can either provide the columns to index via '.option("columnsToIndex", "col_1,col_2")'""" +
          " or enable auto indexing with by setting spark.qbeast.index.autoIndexingEnabled=true")
    }
  }

  override def save(
      data: DataFrame,
      parameters: Map[String, String],
      append: Boolean): BaseRelation = {
    logTrace(s"Begin: Save table $tableID")
    val options = QbeastOptions(verifyAndUpdateParameters(parameters, Some(data)))
    val indexStatus = if (exists && append && hasQbeastMetadata) {
      snapshot.loadLatestIndexStatus
    } else {
      val revision = revisionFactory.createNewRevision(tableID, data.schema, options)
      IndexStatus(revision)
    }
    val result = write(data, indexStatus, options, append)
    logTrace(s"End: Save table $tableID")
    result
  }

  override def load(): BaseRelation = {
    clearCaches()
    createQbeastBaseRelation()
  }

  private def snapshot = {
    if (snapshotCache.isEmpty) {
      snapshotCache = Some(metadataManager.loadSnapshot(tableID))
    }
    snapshotCache.get
  }

  private def clearCaches(): Unit = {
    snapshotCache = None
  }

  /**
   * Creates a QbeastBaseRelation for the given table.
   * @return
   *   the QbeastBaseRelation
   */
  private def createQbeastBaseRelation(): BaseRelation = {
    QbeastBaseRelation.forQbeastTable(this)
  }

  private def write(
      data: DataFrame,
      indexStatus: IndexStatus,
      options: QbeastOptions,
      append: Boolean): BaseRelation = {
    logTrace(s"Begin: Writing data to table $tableID")
    val revision = indexStatus.revision
    logDebug(s"Writing data to table $tableID with revision ${revision.revisionID}")
    var tries = DEFAULT_NUMBER_OF_RETRIES
    while (tries > 0) {
      try {
        doWrite(data, indexStatus, options, append)
        tries = 0
      } catch {
        case cme: ConcurrentModificationException if tries == 0 =>
          // Nothing to do, the conflict is unsolvable
          throw cme
        case _: ConcurrentModificationException =>
          // Trying one more time if the conflict is solvable
          tries -= 1
      }
    }
    clearCaches()
    val result = createQbeastBaseRelation()
    logTrace(s"End: Done writing data to table $tableID")
    result
  }

  private def doWrite(
      data: DataFrame,
      indexStatus: IndexStatus,
      options: QbeastOptions,
      append: Boolean): Unit = {
    logTrace(s"Begin: Writing data to table $tableID")
    val schema = data.schema
    val writeMode = if (append) WriteMode.Append else WriteMode.Overwrite
    metadataManager.updateWithTransaction(tableID, schema, options, writeMode) {
      transactionStartTime: String =>
        val (qbeastData, tableChanges) = indexManager.index(data, indexStatus, options)
        val addFiles =
          dataWriter.write(tableID, schema, qbeastData, tableChanges, transactionStartTime)
        (tableChanges, addFiles, Vector.empty[DeleteFile])
    }
    logTrace(s"End: Writing data to table $tableID")
  }

  /**
   * Selects the unindexed files to optimize based on the fraction
   * @param fraction
   *   the fraction of the data to optimize
   * @return
   */
  private[table] def selectUnindexedFilesToOptimize(fraction: Double): Seq[String] = {
    val revisionFilesDS = snapshot.loadIndexFiles(stagingID)
    // 1. Collect the revision files ordered by modification time
    val revisionFiles = revisionFilesDS.orderBy("modificationTime").collect()
    log.info(s"Total Number of Unindexed Files:  ${revisionFiles.length}")
    // 2. Calculate the total bytes of the files to optimize based on the fraction
    val bytesToOptimize = revisionFiles.map(_.size).sum * fraction
    logInfo(s"Total Bytes of Unindexed Files to Optimize: $bytesToOptimize")
    // 3. Accumulate the files to optimize until the bytesToOptimize is reached
    val filesToOptimize = Seq.newBuilder[String]
    revisionFiles.foldLeft(0L)((acc, file) => {
      if (acc < bytesToOptimize) {
        filesToOptimize += file.path
        acc + file.size
      } else acc
    })
    val filesToOptimizeNames = filesToOptimize.result()
    logInfo(s"Total Number of Unindexed Files to Optimize: ${filesToOptimizeNames.size}")
    filesToOptimizeNames
  }

  /**
   * Selects the indexed files to optimize based on the fraction
   * @param revisionID
   *   the revision identifier
   * @param fraction
   *   the fraction of the data to optimize
   * @return
   */
  private[table] def selectIndexedFilesToOptimize(
      revisionID: RevisionID,
      fraction: Double): Seq[String] = {
    val revisionFilesDS = snapshot.loadIndexFiles(revisionID)
    import revisionFilesDS.sparkSession.implicits._
    val filesToOptimize = revisionFilesDS.transform(filterSamplingFiles(fraction))
    val filesToOptimizeNames = filesToOptimize.map(_.path).collect()
    logInfo(s"Total Number of Indexed Files To Optimize: ${filesToOptimizeNames.length}")
    filesToOptimizeNames
  }

  override def optimize(
      revisionID: RevisionID,
      fraction: Double,
      options: Map[String, String]): Unit = {
    assert(fraction > 0d && fraction <= 1d)
    log.info(s"Selecting Files to Optimize for Revision $revisionID")
    // Filter the Index Files by the fraction
    if (isStaging(revisionID)) { // If the revision is Staging, we should INDEX the staged data up to the fraction
      optimizeUnindexedFiles(selectUnindexedFilesToOptimize(fraction), options)
    } else { // If the revision is not Staging, we should optimize the index files up to the fraction
      optimizeIndexedFiles(selectIndexedFilesToOptimize(revisionID, fraction), options)
    }
  }

  private[table] def filterSamplingFiles(
      fraction: Double): Dataset[IndexFile] => Dataset[IndexFile] = indexFiles => {
    if (fraction == 1.0) indexFiles
    else {
      indexFiles
        .filter(f => f.blocks.exists(_.minWeight.fraction <= fraction))
    }
  }

  override def optimizeUnindexedFiles(
      unindexedFiles: Seq[String],
      options: Map[String, String]): Unit = {
    val unindexedFilesPaths = unindexedFiles.toSet
    if (unindexedFilesPaths.isEmpty) return // Nothing to optimize
    // 1. Load the files from the Staging ID (Unindexed)
    val files =
      snapshot.loadIndexFiles(stagingID).filter(f => unindexedFilesPaths.contains(f.path))
    import files.sparkSession.implicits._
    // 2. Load the Dataframe, the latest index status and the schema
    val filesDF = snapshot.loadDataframeFromIndexFiles(files)
    val latestIndexStatus = snapshot.loadLatestIndexStatus
    val schema = metadataManager.loadCurrentSchema(tableID)
    val optOptions = QbeastOptions(options, latestIndexStatus.revision)
    // 3. In a transaction, update the table with the new data
    metadataManager.updateWithTransaction(tableID, schema, optOptions, WriteMode.Optimize) {
      transactionStartTime: String =>
        // Remove the Unindexed Files from the Log
        val deleteFiles: IISeq[DeleteFile] = files
          .map { indexFile =>
            DeleteFile(
              path = indexFile.path,
              size = indexFile.size,
              dataChange = false,
              deletionTimestamp = currentTimeMillis())
          }
          .collect()
          .toIndexedSeq
        // Index the data with IndexManager
        val (data, tableChanges) = indexManager.index(filesDF, latestIndexStatus, optOptions)
        // Write the data with DataWriter
        val newFiles: IISeq[IndexFile] =
          dataWriter
            .write(tableID, schema, data, tableChanges, transactionStartTime)
            .collect { case indexFile: IndexFile =>
              indexFile.copy(dataChange = false)
            }
            .toIndexedSeq
        // Commit
        (tableChanges, newFiles, deleteFiles)
    }
  }

  override def optimizeIndexedFiles(files: Seq[String], options: Map[String, String]): Unit = {
    if (files.isEmpty) return // Nothing to optimize
    val paths = files.toSet
    val schema = metadataManager.loadCurrentSchema(tableID)
    // For each Revision, excluding the Staging,
    // we should optimize the matching files
    snapshot.loadAllRevisions.filterNot(isStaging).foreach { revision =>
      // 1. Load the Index Files for the given revision
      val indexFiles = snapshot
        .loadIndexFiles(revision.revisionID)
        .filter(file => paths.contains(file.path))
      if (!indexFiles.isEmpty) {
        // 2. Load the Index Status for the given revision
        val indexStatus = snapshot.loadIndexStatus(revision.revisionID)
        // 3. In the same transaction
        metadataManager
          .updateWithTransaction(
            tableID,
            schema,
            QbeastOptions(options, revision),
            WriteMode.Optimize) { transactionStartTime: String =>
            import indexFiles.sparkSession.implicits._
            val deleteFiles: IISeq[DeleteFile] = indexFiles
              .map { indexFile =>
                DeleteFile(
                  path = indexFile.path,
                  size = indexFile.size,
                  dataChange = false,
                  deletionTimestamp = currentTimeMillis())
              }
              .collect()
              .toIndexedSeq
            // 1. Load the data from the Index Files
            val data = snapshot.loadDataframeFromIndexFiles(indexFiles)
            // 2. Optimize the data with IndexManager
            val (dataExtended, tableChanges) = indexManager.optimize(data, indexStatus)
            // 3. Write the data with DataWriter
            val addFiles = dataWriter
              .write(tableID, schema, dataExtended, tableChanges, transactionStartTime)
              .collect { case indexFile: IndexFile =>
                indexFile.copy(dataChange = false)
              }
            dataExtended.unpersist()
            (tableChanges, addFiles, deleteFiles)
          }
      }
    }
  }

}
