package io.qbeast.spark.keeper

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.model._
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, MetadataWriterTest, SparkDeltaMetadataManager}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.ConcurrentModificationException
import java.util.concurrent.CountDownLatch

// scalastyle:off println
trait ConcurrentActor {

  private val internalLatch = new CountDownLatch(1)
  private val externalLatch = new CountDownLatch(1)
  private val thread = new Thread(() => this.run(), this.getClass.getSimpleName)

  def killMe(): Unit = {
    try {
      // I know it is ugly, but scalastyle was nto allowing it, and we need it for testing
      classOf[Thread].getMethod("stop").invoke(thread)
    } catch {
      case _: Throwable => // we don't care
    }
  }

  protected def run(): Unit

  protected def simulatePause(): Unit = {
    internalLatch.await()

  }

  protected def enteredTransaction(): Unit = {
    externalLatch.countDown()
  }

  /**
   * Call this method to be sure the class entered in the transaction.
   */
  def startTransactionAndWait(): Unit = {
    thread.start()
    externalLatch.await()
  }

  def finishTransaction(): Unit = {
    internalLatch.countDown()
    thread.join()
  }

  def writeData(rev: Revision): (TableChanges, Seq[FileAction]) = {
    val tableChanges =
      BroadcastedTableChanges(None, IndexStatus(rev), deltaNormalizedCubeWeights = Map.empty)
    (tableChanges, Seq.empty)
  }

  def optimizeData(
      rev: Revision,
      cubesToOptimize: Set[CubeId]): (TableChanges, Seq[FileAction]) = {
    val tableChanges =
      BroadcastedTableChanges(
        None,
        IndexStatus(rev),
        deltaNormalizedCubeWeights = Map.empty,
        deltaReplicatedSet = cubesToOptimize)
    (tableChanges, Seq.empty)
  }

}

case class ProtoTestContext(outDir: String, spark: SparkSession) {

  lazy val tableID: QTableID = QTableID(outDir + "/q1")

  lazy val schema: StructType = spark.read.format("qbeast").load(tableID.id).schema

  lazy val rev: Revision = DeltaQbeastSnapshot(
    DeltaLog.forTable(spark, tableID.id).snapshot).loadLatestRevision

}

trait ProtocolMockTestSpec extends QbeastIntegrationTestSpec {

  def withContext[T](keeper: Keeper)(testCode: ProtoTestContext => T): T = {
    withQbeastAndSparkContext(keeper) { spark =>
      withTmpDir { outdir =>
        testCode(ProtoTestContext(outdir, spark))
      }
    }
  }

}

class InitProcess(context: ProtoTestContext) extends ConcurrentActor {
  import context.spark.implicits._

  override def run(): Unit = {
    enteredTransaction()
    0.to(1000)
      .map(a => (a, s"$a name"))
      .toDF("id", "name")
      .write
      .format("qbeast")
      .option("columnsToIndex", "id")
      .save(context.tableID.id)

  }

}

class WritingProcess(context: ProtoTestContext)(implicit keeper: Keeper) extends ConcurrentActor {
  import context._
  var succeeded: Option[Boolean] = None

  override def run(): Unit = {
    val winfo = keeper.beginWrite(tableID, rev.revisionID)

    val deltaLog = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID).deltaLog
    val mode = SaveMode.Append
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = MetadataWriterTest(tableID, mode, deltaLog, options, schema)

    var tries = 2
    try {
      // Here new data is written
      val writer = writeData(rev)
      while (tries > 0) {
        val knownAnnounced = winfo.announcedCubes.map(rev.createCubeId)
        deltaLog.withNewTransaction { txn =>
          enteredTransaction()
          val (changes, newFiles) = writer
          val finalActions = metadataWriter.updateMetadata(txn, changes, newFiles)
          simulatePause()
          try {
            txn.commit(finalActions, DeltaOperations.Write(mode))
            tries = 0
            succeeded = Some(true)
          } catch {
            case cme: ConcurrentModificationException
                if SparkDeltaMetadataManager.hasConflicts(
                  tableID,
                  rev.revisionID,
                  knownAnnounced,
                  Set.empty) || tries == 0 =>
              succeeded = Some(false)
              throw cme
            case _: ConcurrentModificationException =>
              tries -= 1
          }
        }
      }

    } finally {
      winfo.end()
    }

  }

}

class OptimizingProcessGood(context: ProtoTestContext)(implicit keeper: Keeper)
    extends ConcurrentActor {

  import context._

  override def run(): Unit = {
    val bo = keeper.beginOptimization(tableID, rev.revisionID)

    val deltaLog = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID).deltaLog
    val mode = SaveMode.Append
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = MetadataWriterTest(tableID, mode, deltaLog, options, schema)
    val cubesToOptimize = bo.cubesToOptimize.map(rev.createCubeId)

    try {
      val optimizer = optimizeData(rev, cubesToOptimize)
      deltaLog.withNewTransaction(tnx => {
        enteredTransaction()
        val (changes, newFiles) = optimizer
        simulatePause()
        val finalActions = metadataWriter.updateMetadata(tnx, changes, newFiles)
        tnx.commit(finalActions, DeltaOperations.ManualUpdate)
        bo.end(cubesToOptimize.map(_.string))
      })
    } catch {
      case _: ConcurrentModificationException =>
        bo.end(Set())
    }
  }

}

class OptimizingProcessBad(context: ProtoTestContext, args: Seq[String])(implicit keeper: Keeper)
    extends ConcurrentActor {

  import context._

  override def run(): Unit = {
    val bo = keeper.beginOptimization(tableID, rev.revisionID)

    val deltaLog = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID).deltaLog
    val mode = SaveMode.Append
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = MetadataWriterTest(tableID, mode, deltaLog, options, schema)

    val cubesToOptimize = args.toSet

    try {
      val optimizer = optimizeData(rev, cubesToOptimize.map(rev.createCubeId))
      deltaLog.withNewTransaction(tnx => {
        enteredTransaction()
        val (changes, newFiles) = optimizer
        simulatePause()
        val finalActions = metadataWriter.updateMetadata(tnx, changes, newFiles)
        tnx.commit(finalActions, DeltaOperations.ManualUpdate)
        bo.end(cubesToOptimize)
      })
    } catch {
      case _: ConcurrentModificationException =>
        bo.end(Set())
    }
  }

}

class AnnouncerProcess(context: ProtoTestContext, args: Seq[String])(implicit keeper: Keeper)
    extends Thread {

  import context._

  override def run(): Unit = {
    keeper.announce(tableID, rev.revisionID, args)
  }

}
