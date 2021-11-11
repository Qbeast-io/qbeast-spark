/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.context.QbeastContext
import io.qbeast.keeper.Keeper
import io.qbeast.model.{
  DataWriter,
  IndexManager,
  IndexStatus,
  MetadataManager,
  QTableID,
  RevisionID
}
import io.qbeast.spark.SparkRevisionBuilder
import io.qbeast.spark.delta.{QbeastOptimizer, QbeastSnapshot, SparkDeltaMetadataManager}
import io.qbeast.spark.index.OTreeAlgorithm
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.delta.actions.{FileAction}
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
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
    new IndexedTableImpl(
      sqlContext,
      tableID,
      keeper,
      oTreeAlgorithm,
      indexManager,
      metadataManager,
      dataWriter)

}

/**
 * Implementation of IndexedTable.
 *
 * @param sqlContext the associated SQLContext
 * @param tableID the table identifier
 * @param keeper the keeper
 * @param oTreeAlgorithm the OTreeAlgorithm instance
 */
private[table] class IndexedTableImpl(
    val sqlContext: SQLContext,
    val tableID: QTableID,
    private val keeper: Keeper,
    private val oTreeAlgorithm: OTreeAlgorithm,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction])
    extends IndexedTable {
  private var deltaLogCache: Option[DeltaLog] = None
  private var snapshotCache: Option[QbeastSnapshot] = None

  override def exists: Boolean = !snapshot.isInitial

  override def save(
      data: DataFrame,
      parameters: Map[String, String],
      append: Boolean): BaseRelation = {
    val indexStatus = if (exists) {
      QbeastContext.metadataManager.loadIndexStatus(tableID)
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
    QbeastBaseRelation(
      new DeltaDataSource().createRelation(sqlContext, Map("path" -> tableID.id)),
      snapshot.lastRevision)
  }

  private def snapshot = {
    if (snapshotCache.isEmpty) {
      snapshotCache = Some(QbeastSnapshot(deltaLog.snapshot))
    }
    snapshotCache.get
  }

  private def deltaLog = {
    if (deltaLogCache.isEmpty) {
      deltaLogCache = Some(DeltaLog.forTable(sqlContext.sparkSession, tableID.id))
    }
    deltaLogCache.get
  }

  private def clearCaches(): Unit = {
    deltaLogCache = None
    snapshotCache = None
  }

  def checkColumnsToMatchSchema(indexStatus: IndexStatus): Unit = {
    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    if (!snapshot.lastRevision.matchColumns(columnsToIndex)) {
      throw AnalysisExceptionFactory.create(
        s"Columns to index '$columnsToIndex' do not match existing index.")
    }
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
    QbeastBaseRelation(deltaLog.createRelation(), revision)
  }

  private def doWrite(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {

    val schema = data.schema

    val codeWrite = {
      val (qbeastData, tableChanges) =
        oTreeAlgorithm.index(data, indexStatus, false)
      val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)
      (tableChanges, fileActions)
    }
    metadataManager.updateWithTransaction(tableID, schema, codeWrite, append)

  }

  private def deltaOptions: DeltaOptions = {
    val parameters = Map("path" -> tableID.id)
    new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
  }

  override def analyze(revisionID: RevisionID): Seq[String] = {
    val indexStatus = metadataManager.loadIndexStatusAt(tableID, revisionID)
    val cubesToAnnounce = oTreeAlgorithm.analyzeIndex(indexStatus).map(_.string)
    keeper.announce(tableID, revisionID, cubesToAnnounce.map(indexStatus.revision.createCubeId))
    cubesToAnnounce

  }

  private def createQbeastOptimizer(revisionID: RevisionID): QbeastOptimizer = {
    val indexStatus = metadataManager.loadIndexStatusAt(tableID, revisionID)
    new QbeastOptimizer(
      tableID,
      deltaLog,
      deltaOptions,
      indexStatus,
      oTreeAlgorithm,
      metadataManager.asInstanceOf[SparkDeltaMetadataManager])
  }

  override def optimize(revisionID: RevisionID): Unit = {

    val bo = keeper.beginOptimization(tableID, revisionID)
    val cubesToOptimize = bo.cubesToOptimize

    val optimizer = createQbeastOptimizer(revisionID)

    val codeOptimize = {
      val (qbeastData, tableChanges) =
        optimizer.optimize(sqlContext.sparkSession, cubesToOptimize)
      val fileActions =
        dataWriter.write(tableID, deltaLog.snapshot.schema, qbeastData, tableChanges)
      (tableChanges, fileActions)
    }

    metadataManager.updateWithTransaction(tableID, deltaLog.snapshot.schema, codeOptimize, true)
    bo.end(cubesToOptimize)
  }

}
