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
package io.qbeast.core.model

import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.core.model.StatefulTestHook.StatefulTestHookState
import io.qbeast.spark.internal.QbeastOptions
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

private class SimpleTestHook extends PreCommitHook {
  override val name: String = "SimpleTestHook"

  var args: Seq[QbeastFile] = Seq.empty

  override def run(args: Seq[QbeastFile]): PreCommitHookOutput = {
    this.args = args
    Map.empty
  }

}

private class StatefulTestHook(val stateId: String) extends PreCommitHook {

  val state: StatefulTestHookState = StatefulTestHook.stateMap(stateId)

  var args: Seq[QbeastFile] = Seq.empty

  override val name: String = "StatefulTestHook"

  override def run(actions: Seq[QbeastFile]): PreCommitHookOutput = {
    this.args = actions
    Map.empty
  }

}

object StatefulTestHook {
  protected case class StatefulTestHookState(timestamp: Long, args: Seq[IndexFile])

  protected var stateMap: Map[String, StatefulTestHookState] = Map.empty

  def createNewState(timestamp: Long, args: Seq[IndexFile]): String = {
    val state = StatefulTestHookState(timestamp, args)
    val stateId = UUID.randomUUID().toString
    stateMap += (stateId -> state)
    stateId
  }

}

class QbeastHookLoaderTest extends AnyFlatSpec with Matchers {

  "loadHook" should "create a simple hook when only the hook class name is provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])
    when(qbeastOptions.hookInfo).thenReturn(
      HookInfo("", classOf[SimpleTestHook].getCanonicalName, None) :: Nil)

    val hookOpts = qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)
    hookOpts.size shouldBe 1
    hookOpts.head shouldBe a[PreCommitHook]

    val mockActions = mock(classOf[List[IndexFile]])
    hookOpts.head.run(mockActions)
    hookOpts.head.asInstanceOf[SimpleTestHook].args shouldBe mockActions
  }

  it should
    "create a stateful hook when both hook class name and arg are provided" in {
      val callingTimestamp = System.currentTimeMillis()
      val argument = StatefulTestHook.createNewState(callingTimestamp, Seq.empty)
      val qbeastOptions = mock(classOf[QbeastOptions])
      when(qbeastOptions.hookInfo).thenReturn(
        HookInfo("", classOf[StatefulTestHook].getCanonicalName, Some(argument)) :: Nil)

      val hooks = qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)
      hooks.size shouldBe 1
      hooks.head shouldBe a[PreCommitHook]

      val mockActions = mock(classOf[List[IndexFile]])
      hooks.head.run(mockActions)
      val sth = hooks.head.asInstanceOf[StatefulTestHook]
      sth.args shouldBe mockActions
      sth.stateId shouldBe argument
      sth.state.timestamp shouldBe callingTimestamp
    }

  it should "return an empty sequence when no hook information is provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])
    when(qbeastOptions.hookInfo).thenReturn(Nil)

    val hookOpts = qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)
    hookOpts.size shouldBe 0
  }

}
