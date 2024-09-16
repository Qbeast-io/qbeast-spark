/*
 * Copyright 2024 Qbeast Analytics, S.L.
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

import io.qbeast.core.model.HookInfo
import io.qbeast.core.model.PreCommitHook
import org.apache.spark.sql.delta.actions.Action

trait DeltaPreCommitHook extends PreCommitHook[Action]

/**
 * A loader for PreCommitHooks
 */
object DeltaHookLoader {

  /**
   * Loads a pre-commit hook from a `HookInfo` object.
   *
   * This method takes a `HookInfo` object and returns a `PreCommitHook` instance.
   *
   * @param hookInfo
   *   The `HookInfo` object representing the hook to load.
   * @return
   *   The loaded `PreCommitHook` instance.
   */
  def loadHook(hookInfo: HookInfo): DeltaPreCommitHook = hookInfo match {
    case HookInfo(_, clsFullName, argOpt) =>
      val cls = Class.forName(clsFullName)
      val instance =
        if (argOpt.isDefined)
          cls.getDeclaredConstructor(argOpt.get.getClass).newInstance(argOpt.get)
        else cls.getDeclaredConstructor().newInstance()
      instance.asInstanceOf[DeltaPreCommitHook]
  }

}
