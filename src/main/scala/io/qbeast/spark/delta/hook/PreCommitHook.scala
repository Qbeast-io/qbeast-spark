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

import io.qbeast.spark.delta.hook.PreCommitHook.getHookArgName
import io.qbeast.spark.delta.hook.PreCommitHook.getHookName
import io.qbeast.spark.delta.hook.PreCommitHook.PreCommitHookOutput
import org.apache.spark.sql.delta.actions.Action

/**
 * A trait representing a pre-commit hook.
 *
 * Pre-commit hooks are executed before a commit is made to the table. They can be used to perform
 * actions such as validation, logging, or other custom logic.
 */
trait PreCommitHook {

  /**
   * The name of the hook.
   *
   * This is used to identify the hook in logs and error messages.
   */
  val name: String

  /**
   * Runs the hook with the given actions.
   *
   * This method takes a sequence of `Action` objects and performs the actions defined by the
   * hook. It returns a `PreCommitHookOutput` which is a map containing the output of the hook.
   *
   * @param actions
   *   The actions to run the hook with.
   * @return
   *   The output of the hook as a `PreCommitHookOutput`.
   */
  def run(actions: Seq[Action]): PreCommitHookOutput

}

/**
 * A companion object for the `PreCommitHook` trait.
 */
object PreCommitHook {

  /**
   * The output of a pre-commit hook.
   */
  type PreCommitHookOutput = Map[String, String]

  val PRE_COMMIT_HOOKS_PREFIX = "qbeastPreCommitHook"

  /**
   * The argument name for hooks.
   */
  private val argName: String = "arg"

  def getHookName(hookName: String): String = s"$PRE_COMMIT_HOOKS_PREFIX.$hookName"

  def getHookArgName(hookName: String): String = s"${getHookName(hookName)}.$argName"

}

/**
 * A loader for PreCommitHooks
 */
object QbeastHookLoader {

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
  def loadHook(hookInfo: HookInfo): PreCommitHook = hookInfo match {
    case HookInfo(_, clsFullName, argOpt) =>
      val cls = Class.forName(clsFullName)
      val instance =
        if (argOpt.isDefined)
          cls.getDeclaredConstructor(argOpt.get.getClass).newInstance(argOpt.get)
        else cls.getDeclaredConstructor().newInstance()
      instance.asInstanceOf[PreCommitHook]
  }

}

/**
 * A case class representing information about a hook.
 *
 * This class contains the full class name of the hook and an optional argument to be passed to
 * the hook. The argument is represented as an optional string.
 *
 * @param clsFullName
 *   The full class name of the hook.
 * @param arg
 *   An optional argument to be passed to the hook.
 */
case class HookInfo(name: String, clsFullName: String, arg: Option[String]) {

  /**
   * Converts the `HookInfo` object to a map.
   *
   * This method converts the `HookInfo` object to a map where the key is the hook name and the
   * value is the full class name of the hook. If an argument is present, it is added to the map
   * with the key being the hook name followed by ".arg".
   * @return
   *   The `HookInfo` object as a map.
   */
  def toMap: Map[String, String] = {
    val hookName = getHookName(name)
    if (arg.isEmpty) Map(hookName -> clsFullName)
    else Map(hookName -> clsFullName, getHookArgName(name) -> arg.get)
  }

}
