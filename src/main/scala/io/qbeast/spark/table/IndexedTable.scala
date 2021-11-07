/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.context.QbeastContext
import io.qbeast.keeper.Keeper
import io.qbeast.model.{EmptyIndexStatus, IndexStatus, QTableID, RevisionID}
import io.qbeast.spark.SparkRevisionBuilder
import io.qbeast.spark.delta.{QbeastOptimizer, QbeastSnapshot, QbeastWriter}
import io.qbeast.spark.index.OTreeAlgorithm
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.delta.actions.SetTransaction
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame, SQLContext, SaveMode}

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
   * @param columnsToIndex the columns to index
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
    private val oTreeAlgorithm: OTreeAlgorithm)
    extends IndexedTableFactory {

  override def getIndexedTable(sqlContext: SQLContext, tableID: QTableID): IndexedTable =
    new IndexedTableImpl(sqlContext, tableID, keeper, oTreeAlgorithm)

}

/**
 * Implementation of IndexedTable.
 *
 * @param sqlContext the associated SQLContext
 * @param path the table path
 * @param keeper the keeper
 * @param oTreeAlgorithm the OTreeAlgorithm instance
 */
private[table] class IndexedTableImpl(
    val sqlContext: SQLContext,
    val tableID: QTableID,
    private val keeper: Keeper,
    private val oTreeAlgorithm: OTreeAlgorithm)
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
    } else {
      EmptyIndexStatus(SparkRevisionBuilder.createNewRevision(tableID, data, parameters))
    }

    if (exists && append) {
      checkColumnsToMatchSchema(indexStatus)
    }

    // TODO add the newRevision built from the dataframe and columnToIndex.
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
          .map(revision.createCubeId)

        val updatedStatus = indexStatus.addAnnouncements(announcedSet)
        doWrite(data, updatedStatus, append)
      }
    } else {
      doWrite(data, indexStatus, append)
    }
    QbeastBaseRelation(deltaLog.createRelation(), revision)
  }

  private def doWrite(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options = deltaOptions
    val writer =
      createQbeastWriter(data, indexStatus, mode, options)
    val deltaWrite = createDeltaWrite(mode, options)
    deltaLog.withNewTransaction { transaction =>
      val actions = writer.write(transaction, sqlContext.sparkSession)
      transaction.commit(actions, deltaWrite)
    }
  }

  private def deltaOptions: DeltaOptions = {
    val parameters = Map("path" -> tableID.id)
    new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
  }

  private def createQbeastWriter(
      data: DataFrame,
      indexStatus: IndexStatus,
      mode: SaveMode,
      options: DeltaOptions): QbeastWriter = {
    QbeastWriter(
      mode = mode,
      deltaLog = deltaLog,
      options = options,
      partitionColumns = Nil,
      data = data,
      indexStatus = indexStatus,
      qbeastSnapshot = snapshot,
      oTreeAlgorithm = oTreeAlgorithm)
  }

  private def createDeltaWrite(
      mode: SaveMode,
      options: DeltaOptions): DeltaOperations.Operation = {
    DeltaOperations.Write(mode, None, options.replaceWhere, options.userMetadata)
  }

  override def analyze(revisionID: RevisionID): Seq[String] = {
    val revisionData = snapshot.getRevisionData(revisionID)
    val cubesToAnnounce = oTreeAlgorithm.analyzeIndex(revisionData).map(_.string)
    keeper.announce(tableID, revisionID, cubesToAnnounce)
    cubesToAnnounce

  }

  private def createQbeastOptimizer(revisionID: RevisionID): QbeastOptimizer = {
    new QbeastOptimizer(deltaLog, deltaOptions, snapshot, revisionID, oTreeAlgorithm)
  }

  override def optimize(revisionID: RevisionID): Unit = {

    val bo = keeper.beginOptimization(tableID, revisionID)
    val cubesToOptimize = bo.cubesToOptimize

    val optimizer = createQbeastOptimizer(revisionID)
    val deltaWrite =
      createDeltaWrite(SaveMode.Append, deltaOptions)
    val transactionID = s"qbeast.$tableID"

    deltaLog.withNewTransaction { txn =>
      val startingTnx = txn.txnVersion(transactionID)
      val newTransaction = startingTnx + 1
      val transRecord =
        SetTransaction(transactionID, newTransaction, Some(System.currentTimeMillis()))

      val (replicatedCubeIds, actions) =
        optimizer.optimize(txn, sqlContext.sparkSession, cubesToOptimize)

      txn.commit(actions ++ Seq(transRecord), deltaWrite)
      bo.end(replicatedCubeIds.map(_.string))
    }
  }

}
