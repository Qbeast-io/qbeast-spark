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
package io.qbeast.spark.delta.hook

import io.qbeast.spark.delta.hook.StatefulTestHook.StatefulTestHookState
import io.qbeast.spark.internal.QbeastOptions
import org.apache.spark.sql.delta.actions.Action
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SimpleTestHook extends QbeastPreCommitHook {
  override val name: String = "SimpleTestHook"

  var args: Seq[Action] = Seq.empty

  override def run(args: Seq[Action]): Unit = {
    this.args = args
  }

}

class StatefulTestHook(val stateId: String) extends QbeastPreCommitHook {

  val state: StatefulTestHookState = StatefulTestHook.stateMap(stateId)

  var args: Seq[Action] = Seq.empty

  override val name: String = "StatefulTestHook"

  override def run(actions: Seq[Action]): Unit = {
    this.args = actions
  }

}

object StatefulTestHook {
  protected case class StatefulTestHookState(timestamp: Long, args: Seq[Action])

  protected var stateMap: Map[String, StatefulTestHookState] = Map.empty

  def createNewState(timestamp: Long, args: Seq[Action]): String = {
    val state = StatefulTestHookState(timestamp, args)
    val stateId = UUID.randomUUID().toString
    stateMap += (stateId -> state)
    stateId
  }

}

class QbeastHookLoaderTest extends AnyFlatSpec with Matchers {

  "loadHook" should "create a simple hook when only the 'hookClassName' is provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])
    when(qbeastOptions.hookClassName).thenReturn(Some(classOf[SimpleTestHook].getCanonicalName))
    when(qbeastOptions.hookArgument).thenReturn(None)

    val hookOpt = QbeastHookLoader.loadHook(qbeastOptions)
    hookOpt.isDefined shouldBe true
    hookOpt.get shouldBe a[QbeastPreCommitHook]

    val mockActions = mock(classOf[List[Action]])
    hookOpt.get.run(mockActions)
    hookOpt.get.asInstanceOf[SimpleTestHook].args shouldBe mockActions
  }

  it should "create a stateful hook when both the 'hookClassName' and 'hookArgument' are provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])

    when(qbeastOptions.hookClassName).thenReturn(Some(classOf[StatefulTestHook].getCanonicalName))
    val callingTimestamp = System.currentTimeMillis()
    val argument = StatefulTestHook.createNewState(callingTimestamp, Seq.empty)
    when(qbeastOptions.hookArgument).thenReturn(Some(argument))

    val hookOpt = QbeastHookLoader.loadHook(qbeastOptions)
    hookOpt.isDefined shouldBe true
    hookOpt.get shouldBe a[QbeastPreCommitHook]
    val mockActions = mock(classOf[List[Action]])
    hookOpt.get.run(mockActions)
    val sth = hookOpt.get.asInstanceOf[StatefulTestHook]
    sth.args shouldBe mockActions
    sth.stateId shouldBe argument
    sth.state.timestamp shouldBe callingTimestamp

  }

  it should "return None when neither hookClassName nor hookStateId are provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])
    when(qbeastOptions.hookClassName).thenReturn(None)
    when(qbeastOptions.hookArgument).thenReturn(None)

    val result = QbeastHookLoader.loadHook(qbeastOptions)
    result.isDefined shouldBe false
  }

  it should "throw ClassNotFoundException when an invalid hookClassName is provided" in {
    val qbeastOptions = mock(classOf[QbeastOptions])
    when(qbeastOptions.hookClassName).thenReturn(Some("invalid.ClassName"))
    when(qbeastOptions.hookArgument).thenReturn(None)

    assertThrows[ClassNotFoundException] { QbeastHookLoader.loadHook(qbeastOptions) }
  }

}
