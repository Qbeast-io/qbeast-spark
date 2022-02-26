/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.model._
import io.qbeast.spark.delta.CubeDataLoader
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame}

import java.util.ConcurrentModificationException

/**
 * Indexed table represents the tabular data storage
 * indexed with the OTree indexing technology.
 */
trait IndexedTable {

  /**
   * Returns whether the table physically exists.
   * @return the table physically exists.
   */
  def exists: Boolean

  /**
   * Returns the table id which identifies the table.
   *
   * @return the table id
   */
  def tableID: QTableID

  /**
   * Saves given data in the table and updates the index. The specified columns are
   * used to define the index when the table is created or overwritten. The append
   * flag defines whether the existing data should be overwritten.
   *
   * @param data the data to save
   * @param parameters the parameters to save the data
   * @param append the data should be appended to the table
   * @return the base relation to read the saved data
   */
  def save(data: DataFrame, parameters: Map[String, String], append: Boolean): BaseRelation

  /**
   * Loads the table data.
   *
   * @return the base relation to read the table data
   */
  def load(): BaseRelation

  /**
   * Analyzes the index for a given revision
   * @param revisionID the identifier of revision to analyze
   * @return the cubes to analyze
   */
  def analyze(revisionID: RevisionID): Seq[String]

  /**
   * Optimizes the given table for a given revision
   * @param revisionID the identifier of revision to optimize
   */
  def optimize(revisionID: RevisionID): Unit
}

/**
 * IndexedTable factory.
 */
trait IndexedTableFactory {

  /**
   * Returns a IndexedTable for given SQLContext and path.
   * It is not guaranteed that the returned table physically
   * exists, use IndexedTable#exists attribute to verify it.
   *
   * @param tableId the table path
   * @return the table
   */
  def getIndexedTable(tableId: QTableID): IndexedTable
}

/**
 * Implementation of IndexedTableFactory.
 * @param keeper the keeper
 * @param indexManager the index manager
 * @param metadataManager the metadata manager
 * @param dataWriter the data writer
 * @param revisionBuilder the revision builder
 */
final class IndexedTableFactoryImpl(
    private val keeper: Keeper,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction],
    private val revisionBuilder: RevisionFactory[StructType])
    extends IndexedTableFactory {

  override def getIndexedTable(tableID: QTableID): IndexedTable =
    new IndexedTableImpl(
      tableID,
      keeper,
      indexManager,
      metadataManager,
      dataWriter,
      revisionBuilder)

}

/**
 * Implementation of IndexedTable.
 *
 * @param tableID the table identifier
 * @param keeper the keeper
 * @param indexManager the index manager
 * @param metadataManager the metadata manager
 * @param dataWriter the data writer
 * @param revisionBuilder the revision builder
 */
private[table] class IndexedTableImpl(
    val tableID: QTableID,
    private val keeper: Keeper,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction],
    private val revisionBuilder: RevisionFactory[StructType])
    extends IndexedTable {
  private var snapshotCache: Option[QbeastSnapshot] = None

  override def exists: Boolean = !snapshot.isInitial

  private def checkRevisionParameters(
      qbeastOptions: QbeastOptions,
      latestRevision: Revision): Boolean = {
    // TODO feature: columnsToIndex may change between revisions
    latestRevision.desiredCubeSize == qbeastOptions.cubeSize

  }

  override def save(
      data: DataFrame,
      parameters: Map[String, String],
      append: Boolean): BaseRelation = {
    val indexStatus =
      if (exists) {
        val latestIndexStatus = snapshot.loadLatestIndexStatus
        if (checkRevisionParameters(QbeastOptions(parameters), latestIndexStatus.revision)) {
          latestIndexStatus
        } else {
          val oldRevisionID = latestIndexStatus.revision.revisionID
          val newRevision = revisionBuilder
            .createNextRevision(tableID, data.schema, parameters, oldRevisionID)
          IndexStatus(newRevision)
        }
      } else {
        IndexStatus(revisionBuilder.createNewRevision(tableID, data.schema, parameters))
      }

    if (exists && append) {
      checkColumnsToMatchSchema(indexStatus)
    }

    val relation = write(data, indexStatus, append)
    relation
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

  private def checkColumnsToMatchSchema(indexStatus: IndexStatus): Unit = {
    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    if (!snapshot.loadLatestRevision.matchColumns(columnsToIndex)) {
      throw AnalysisExceptionFactory.create(
        s"Columns to index '$columnsToIndex' do not match existing index.")
    }
  }

  private def createQbeastBaseRelation(): QbeastBaseRelation = {
    QbeastBaseRelation.forDeltaTable(tableID)
  }

  private def write(data: DataFrame, indexStatus: IndexStatus, append: Boolean): BaseRelation = {
    val revision = indexStatus.revision

    if (exists) {
      keeper.withWrite(tableID.id, revision.revisionID) { write =>
        val announcedSet = write.announcedCubes.map(revision.createCubeId)
        val updatedStatus = indexStatus.addAnnouncements(announcedSet)
        doWrite(data, updatedStatus, append)
      }
    } else {
      keeper.withWrite(tableID.id, revision.revisionID) { write =>
        doWrite(data, indexStatus, append)
      }
    }
    clearCaches()
    createQbeastBaseRelation()
  }

  private def doWrite(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {

    val schema = data.schema
    val revisionID = indexStatus.revision.revisionID
    val oldAnnouncedSet = indexStatus.announcedSet
    val oldReplicatedSet = indexStatus.replicatedSet

    var tries = 2
    try {
      while (tries > 0) {
        try {
          // Try to commit transaction
          metadataManager.updateWithTransaction(tableID, schema, append) {
            val (qbeastData, tableChanges) =
              indexManager.index(data, indexStatus)
            val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)
            (tableChanges, fileActions)
          }
          tries = 0
        } catch {
          case cme: ConcurrentModificationException
              if metadataManager.isConflicted(
                tableID,
                revisionID,
                oldReplicatedSet,
                oldAnnouncedSet) || tries == 0 =>
            // Nothing to do, the conflict is unsolvable
            throw cme

          case _: ConcurrentModificationException =>
            // Trying one more time if the conflict is solvable
            tries -= 1

        }
      }
    }
  }

  override def analyze(revisionID: RevisionID): Seq[String] = {
    val indexStatus = snapshot.loadIndexStatus(revisionID)
    val cubesToAnnounce = indexManager.analyze(indexStatus).map(_.string)
    keeper.announce(tableID.id, revisionID, cubesToAnnounce)
    cubesToAnnounce

  }

  override def optimize(revisionID: RevisionID): Unit = {

    // begin keeper transaction
    val bo = keeper.beginOptimization(tableID.id, revisionID)

    val currentIndexStatus = snapshot.loadIndexStatus(revisionID)
    val cubesToReplicate = bo.cubesToOptimize.map(currentIndexStatus.revision.createCubeId)
    val indexStatus = currentIndexStatus.addAnnouncements(cubesToReplicate)
    val schema = metadataManager.loadCurrentSchema(tableID)

    if (cubesToReplicate.nonEmpty) {
      try {
        // Try to commit transaction
        metadataManager.updateWithTransaction(tableID, schema, append = true) {
          val dataToReplicate =
            CubeDataLoader(tableID).loadSetWithCubeColumn(
              cubesToReplicate,
              indexStatus.revision,
              QbeastColumns.cubeToReplicateColumnName)
          val (qbeastData, tableChanges) =
            indexManager.optimize(dataToReplicate, indexStatus)
          val fileActions =
            dataWriter.write(tableID, schema, qbeastData, tableChanges)
          (tableChanges, fileActions)
        }
      } catch {
        case _: ConcurrentModificationException =>
          bo.end(Set())
      } finally {
        // end keeper transaction
        bo.end(cubesToReplicate.map(_.string))
      }
    } else {
      // end keeper transaction
      bo.end(Set())
    }

    clearCaches()
  }

}
