/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.spark.index.{ColumnsToIndex, CubeId, OTreeAlgorithm}
import io.qbeast.spark.keeper.Keeper
import io.qbeast.spark.model.RevisionID
import io.qbeast.spark.sql.qbeast.{QbeastOptimizer, QbeastSnapshot, QbeastWriter}
import io.qbeast.spark.sql.sources.QbeastBaseRelation
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.SetTransaction
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.NumericType
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
   * Returns the table path which identifies the table.
   *
   * @return the table path
   */
  def path: String

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
  def save(data: DataFrame, columnsToIndex: Seq[String], append: Boolean): BaseRelation

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
   * @param path the table path
   * @return the table
   */
  def getIndexedTable(sqlContext: SQLContext, path: String): IndexedTable
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

  override def getIndexedTable(sqlContext: SQLContext, path: String): IndexedTable =
    new IndexedTableImpl(sqlContext, path, keeper, oTreeAlgorithm)

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
    val path: String,
    private val keeper: Keeper,
    private val oTreeAlgorithm: OTreeAlgorithm)
    extends IndexedTable {
  private var deltaLogCache: Option[DeltaLog] = None
  private var snapshotCache: Option[QbeastSnapshot] = None

  override def exists: Boolean = !snapshot.isInitial

  override def save(
      data: DataFrame,
      columnsToIndex: Seq[String],
      append: Boolean): BaseRelation = {
    checkColumnsToIndexMatchData(data, columnsToIndex)
    if (exists && append) {
      checkColumnsToMatchSchema(columnsToIndex)
    }
    val relation = write(data, columnsToIndex, append)
    clearCaches()
    relation
  }

  override def load(): BaseRelation = {
    QbeastBaseRelation(
      new DeltaDataSource().createRelation(sqlContext, Map("path" -> path)),
      snapshot.indexedCols)
  }

  private def snapshot = {
    if (snapshotCache.isEmpty) {
      snapshotCache = Some(QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize))
    }
    snapshotCache.get
  }

  private def deltaLog = {
    if (deltaLogCache.isEmpty) {
      deltaLogCache = Some(DeltaLog.forTable(sqlContext.sparkSession, path))
    }
    deltaLogCache.get
  }

  private def clearCaches(): Unit = {
    deltaLogCache = None
    snapshotCache = None
  }

  private def checkColumnsToIndexMatchData(data: DataFrame, columnsToIndex: Seq[String]): Unit = {
    val dataTypes = data.schema.fields.map(field => field.name -> field.dataType).toMap
    for (column <- columnsToIndex) {
      val dataType = dataTypes.getOrElse(
        column, {
          throw AnalysisExceptionFactory.create(s"Column '$column' is not found in data.")
        })
      if (!dataType.isInstanceOf[NumericType]) {
        throw AnalysisExceptionFactory.create(s"Column '$column' is not numeric.")
      }
    }
  }

  private def checkColumnsToMatchSchema(columnsToIndex: Seq[String]): Unit = {
    if (!ColumnsToIndex.areSame(
        columnsToIndex,
        snapshot.lastRevisionData.revision.dimensionColumns)) {
      throw AnalysisExceptionFactory.create(
        s"Columns to index '$columnsToIndex' do not match existing index.")
    }
  }

  private def createQbeastLogIfNecessary(): Unit = {
    val configuration = sqlContext.sparkSession.sessionState.newHadoopConf()
    val path = new Path(deltaLog.dataPath, "_qbeast")
    val fileSystem = path.getFileSystem(configuration)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
      fileSystem.makeQualified(path)
    }
  }

  private def write(
      data: DataFrame,
      columnsToIndex: Seq[String],
      append: Boolean): BaseRelation = {
    createQbeastLogIfNecessary()
    val dimensionCount = columnsToIndex.length
    if (exists) {
      val revision = snapshot.lastRevisionData.revision
      keeper.withWrite(path, revision.timestamp) { write =>
        val announcedSet = write.announcedCubes
          .map(CubeId(dimensionCount, _))
        doWrite(data, columnsToIndex, append, announcedSet)
      }
    } else {
      doWrite(data, columnsToIndex, append, Set.empty)
    }
    QbeastBaseRelation(deltaLog.createRelation(), columnsToIndex)
  }

  private def doWrite(
      data: DataFrame,
      columnsToIndex: Seq[String],
      append: Boolean,
      announcedSet: Set[CubeId]): Unit = {
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options = deltaOptions
    val writer =
      createQbeastWriter(data, columnsToIndex, mode, announcedSet, options)
    val deltaWrite = createDeltaWrite(mode, options)
    deltaLog.withNewTransaction { transaction =>
      val actions = writer.write(transaction, sqlContext.sparkSession)
      transaction.commit(actions, deltaWrite)
    }
  }

  private def deltaOptions: DeltaOptions = {
    val parameters = Map("path" -> path)
    new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
  }

  private def createQbeastWriter(
      data: DataFrame,
      columnsToIndex: Seq[String],
      mode: SaveMode,
      announcedSet: Set[CubeId],
      options: DeltaOptions): QbeastWriter = {
    QbeastWriter(
      mode = mode,
      deltaLog = deltaLog,
      options = options,
      partitionColumns = Nil,
      data = data,
      columnsToIndex = columnsToIndex,
      qbeastSnapshot = snapshot,
      announcedSet = announcedSet,
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
    keeper.announce(path, revisionID, cubesToAnnounce)
    cubesToAnnounce

  }

  private def createQbeastOptimizer(revisionID: RevisionID): QbeastOptimizer = {
    new QbeastOptimizer(deltaLog, deltaOptions, snapshot, revisionID, oTreeAlgorithm)
  }

  override def optimize(revisionID: RevisionID): Unit = {

    val bo = keeper.beginOptimization(path, revisionID)
    val cubesToOptimize = bo.cubesToOptimize

    val optimizer = createQbeastOptimizer(revisionID)
    val deltaWrite =
      createDeltaWrite(SaveMode.Append, deltaOptions)
    val transactionID = s"qbeast.$path"

    deltaLog.withNewTransaction { txn =>
      val startingTnx = txn.txnVersion(transactionID)
      val newTransaction = startingTnx + 1
      val transRecord =
        SetTransaction(transactionID, newTransaction, Some(System.currentTimeMillis()))

      val (replicatedCubeIds, actions) =
        optimizer.optimize(sqlContext.sparkSession, cubesToOptimize)
      optimizer.updateReplicatedSet(sqlContext.sparkSession, newTransaction, replicatedCubeIds)

      txn.commit(actions ++ Seq(transRecord), deltaWrite)
      bo.end(replicatedCubeIds.map(_.string))
    }
  }

}
