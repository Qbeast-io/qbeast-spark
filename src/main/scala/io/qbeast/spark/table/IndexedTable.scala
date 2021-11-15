/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.keeper.Keeper
import io.qbeast.model.api.{DataWriter, IndexManager, MetadataManager, QbeastSnapshot}
import io.qbeast.model.{IndexStatus, QTableID, RevisionID}
import io.qbeast.spark.SparkRevisionBuilder
import io.qbeast.spark.delta.DataLoader
import io.qbeast.spark.index.OTreeAlgorithm
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame, SQLContext}

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
   * Returns the associated SQLContext.
   *
   * @return the associated SQLContext
   */
  def sqlContext: SQLContext

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
   * @param sqlContext the SQLContext to associate with the table
   * @param tableId the table path
   * @return the table
   */
  def getIndexedTable(sqlContext: SQLContext, tableId: QTableID): IndexedTable
}

/**
 * Implementation of IndexedTableFactory.
 *
 * @param keeper the keeper
 * @param oTreeAlgorithm the OTreeAlgorithm instance
 */
final class IndexedTableFactoryImpl(
    private val keeper: Keeper,
    private val oTreeAlgorithm: OTreeAlgorithm,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction])
    extends IndexedTableFactory {

  override def getIndexedTable(sqlContext: SQLContext, tableID: QTableID): IndexedTable =
    new IndexedTableImpl(sqlContext, tableID, keeper, indexManager, metadataManager, dataWriter)

}

/**
 * Implementation of IndexedTable.
 *
 * @param sqlContext the associated SQLContext
 * @param tableID the table identifier
 * @param keeper the keeper
 */
private[table] class IndexedTableImpl(
    val sqlContext: SQLContext,
    val tableID: QTableID,
    private val keeper: Keeper,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction])
    extends IndexedTable {
  private var snapshotCache: Option[QbeastSnapshot] = None

  override def exists: Boolean = !snapshot.isInitial

  override def save(
      data: DataFrame,
      parameters: Map[String, String],
      append: Boolean): BaseRelation = {
    val indexStatus = if (exists) {
      snapshot.loadLatestIndexStatus
      // TODO check here if the parameters is compatible with the old revision
    } else {
      IndexStatus(SparkRevisionBuilder.createNewRevision(tableID, data, parameters))
    }

    if (exists && append) {
      checkColumnsToMatchSchema(indexStatus)
    }

    val relation = write(data, indexStatus, append)
    clearCaches()
    relation
  }

  override def load(): BaseRelation = {
    clearCaches()
    createQbeastBaseRelation()
  }

  private def snapshot = {
    if (snapshotCache.isEmpty) {
      snapshotCache = Some(metadataManager.loadQbeastSnapshot(tableID))
    }
    snapshotCache.get
  }

  private def clearCaches(): Unit = {
    snapshotCache = None
  }

  def checkColumnsToMatchSchema(indexStatus: IndexStatus): Unit = {
    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    if (!snapshot.loadLatestRevision.matchColumns(columnsToIndex)) {
      throw AnalysisExceptionFactory.create(
        s"Columns to index '$columnsToIndex' do not match existing index.")
    }
  }

  private def createQbeastBaseRelation(): QbeastBaseRelation = {

    QbeastBaseRelation.forTableID(sqlContext.sparkSession, tableID)
  }

  private def write(data: DataFrame, indexStatus: IndexStatus, append: Boolean): BaseRelation = {
    val revision = indexStatus.revision

    if (exists) {
      keeper.withWrite(indexStatus.revision.tableID, revision.revisionID) { write =>
        val announcedSet = write.announcedCubes
        val updatedStatus = indexStatus.addAnnouncements(announcedSet)
        doWrite(data, updatedStatus, append)
      }
    } else {
      doWrite(data, indexStatus, append)
    }
    clearCaches()
    createQbeastBaseRelation()
  }

  private def doWrite(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {

    val schema = data.schema
    metadataManager.updateWithTransaction(tableID, schema, append) {
      val (qbeastData, tableChanges) =
        indexManager.index(data, indexStatus)
      val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)
      (tableChanges, fileActions)
    }

  }

  override def analyze(revisionID: RevisionID): Seq[String] = {
    val indexStatus = snapshot.loadIndexStatusAt(revisionID)
    val cubesToAnnounce = indexManager.analyze(indexStatus)
    keeper.announce(tableID, revisionID, cubesToAnnounce)
    cubesToAnnounce.map(_.string)

  }

  override def optimize(revisionID: RevisionID): Unit = {

    val bo = keeper.beginOptimization(tableID, revisionID)
    val currentIndexStatus = snapshot.loadIndexStatusAt(revisionID)
    val indexStatus = currentIndexStatus.addAnnouncements(bo.cubesToOptimize)
    val cubesToReplicate = indexStatus.cubesToOptimize
    val schema = metadataManager.loadCurrentSchema(tableID)

    if (cubesToReplicate.nonEmpty) {
      metadataManager.updateWithTransaction(tableID, schema, true) {
        val dataToReplicate =
          DataLoader(tableID).loadSetWithCubeColumn(cubesToReplicate, indexStatus.revision)

        val (qbeastData, tableChanges) =
          indexManager.optimize(dataToReplicate, indexStatus)
        val fileActions =
          dataWriter.write(tableID, schema, qbeastData, tableChanges)
        (tableChanges, fileActions)
      }
    }

    bo.end(cubesToReplicate)
    clearCaches()
  }

}
