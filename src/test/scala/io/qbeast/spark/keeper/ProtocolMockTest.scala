package io.qbeast.spark.keeper

import io.qbeast.core.keeper.{Keeper, LocalKeeper}

class ProtocolMockTest extends ProtocolMockTestSpec {
  "the qbeast-spark client" should
    "throw an execution when an inconstant state is found" in withContext(LocalKeeper) {
      context =>
        implicit val keeper: Keeper = LocalKeeper

        val initProcess = new InitProcess(context)
        val announcer = new AnnouncerProcess(context, Seq("", "A", "AA", "AAA"))
        val writer = new WritingProcess(context)
        val badOptimizer = new OptimizingProcessBad(context, Seq("gA", "g"))

        initProcess.startTransactionAndWait()
        initProcess.finishTransaction()

        announcer.start()
        announcer.join()

        writer.startTransactionAndWait()

        badOptimizer.startTransactionAndWait()

        badOptimizer.finishTransaction()

        writer.finishTransaction()

        writer.succeeded shouldBe Some(false)

    }
  "A faulty keeper" should "not cause inconsistency with conflicts" in withContext(RandomKeeper) {
    context =>
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

      optim.startTransactionAndWait()

      optim.finishTransaction()

      writer.finishTransaction()
      writer.succeeded shouldBe Some(false)

  }

  it should "not cause inconsistency when there are not conflicts" in withContext(RandomKeeper) {
    context =>
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

  "A write timout" should
    "not cause inconsistency when a a timeout may interfere with an optimization" in withContext(
      LocalKeeper) { context =>
      implicit val keeper = LocalKeeper
      val initProcess = new InitProcess(context)
      val announcer = new AnnouncerProcess(context, Seq("", "A", "AA"))
      val writer = new WritingProcess(context)
      val optim = new OptimizingProcessGood(context)

      initProcess.startTransactionAndWait()
      initProcess.finishTransaction()

      writer.startTransactionAndWait()
      Thread.sleep(1000) // We make sure the keeper forgot about this write operations

      announcer.start() // so that when we announce, we are not aware of a running write operation
      announcer.join()

      // which should lead the optim to optimize something it should not be touched.

      optim.startTransactionAndWait()
      optim.finishTransaction()

      // But the write should detect it and fail
      writer.finishTransaction()
      writer.succeeded shouldBe Some(false)

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
