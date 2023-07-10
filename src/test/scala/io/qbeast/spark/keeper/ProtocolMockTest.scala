package io.qbeast.spark.keeper

import io.qbeast.core.keeper.{Keeper, LocalKeeper}

class ProtocolMockTest extends ProtocolMockTestSpec {

  "The qbeast-spark client" should "not cause inconsistency when there are not conflicts" in
    withContext(RandomKeeper) { context =>
      implicit val keeper: Keeper = RandomKeeper
      val initProcess = new InitProcess(context)
      val announcer = new AnnouncerProcess(context, Seq("", "A", "AA", "AAA"))
      val writer = new WritingProcess(context)
      val optim = new OptimizingProcessGood(context)

      initProcess.startTransactionAndWait()
      initProcess.finishTransaction()

      announcer.start()
      announcer.join()

      writer.startTransactionAndWait()
      writer.finishTransaction()
      optim.startTransactionAndWait()

      optim.finishTransaction()

      writer.succeeded shouldBe Some(true)

    }

  "A crashed with timeouts" should "not cause inconsistency in normal scenario" in withContext(
    LocalKeeper) { context =>
    implicit val keeper: Keeper = LocalKeeper
    val initProcess = new InitProcess(context)
    val announcer = new AnnouncerProcess(context, Seq("", "A", "AA"))
    val writer = new WritingProcess(context)
    val optim = new OptimizingProcessGood(context)

    initProcess.startTransactionAndWait()
    initProcess.finishTransaction()

    announcer.start()
    announcer.join()

    writer.startTransactionAndWait()
    writer.finishTransaction()
    optim.startTransactionAndWait()

    optim.finishTransaction()

    writer.succeeded shouldBe Some(true)
  }

  "A crashed optimization" should "not caused problems" in withContext(LocalKeeper) { context =>
    implicit val keeper = LocalKeeper

    val initProcess = new InitProcess(context)
    val announcer = new AnnouncerProcess(context, Seq("", "A", "AA"))
    val writer = new WritingProcess(context)
    val optim1 = new OptimizingProcessGood(context)

    initProcess.startTransactionAndWait()
    initProcess.finishTransaction()

    writer.startTransactionAndWait()

    announcer.start() // so that when we announce, we are not aware of a running write operation
    announcer.join()

    // which should lead the optim to optimize something it should not be touched.

    optim1.startTransactionAndWait()
    optim1.killMe()

    Thread.sleep(1000) // this should ensure the client cleans the pending optimization
    writer.finishTransaction()
    writer.succeeded shouldBe Some(true)

  }

}
