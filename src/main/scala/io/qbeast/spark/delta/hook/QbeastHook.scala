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

import io.qbeast.spark.internal.QbeastOptions
import org.apache.spark.sql.delta.actions.Action

/**
 * A hook that can be run before a commit
 */
trait QbeastPreCommitHook {

  /**
   * The name of the hook
   */
  val name: String

  /**
   * Run the hook with the given actions
   *
   * @param actions
   *   the actions to run the hook with
   */
  def run(actions: Seq[Action]): Unit
}

/**
 * A loader for QbeastPreCommitHooks
 */
object QbeastHookLoader {

  def loadHook(qbeastOptions: QbeastOptions): Option[QbeastPreCommitHook] = {
    (qbeastOptions.hookClassName, qbeastOptions.hookArgument) match {
      case (Some(hookClassName), Some(hookArgument)) =>
        val hook = Class
          .forName(hookClassName)
          .getDeclaredConstructor(classOf[String])
          .newInstance(hookArgument)
          .asInstanceOf[QbeastPreCommitHook]
        Some(hook)
      case (Some(hookClassName), None) =>
        val hook = Class
          .forName(hookClassName)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[QbeastPreCommitHook]
        Some(hook)
      case _ => None
    }

  }

}
