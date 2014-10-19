/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.job

import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.ShellCommandConfig.Config2ShellCommand
import org.apache.samza.util.JsonHelpers
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.util.Util
import scala.collection.JavaConversions._

class ShellCommandBuilder extends CommandBuilder {
  def buildCommand() = config.getCommand

  def buildEnvironment(): java.util.Map[String, String] = {
    import JsonHelpers._

    var streamsAndPartsString = serializeSystemStreamPartitionSetToJSON(taskNameToSystemStreamPartitionsMapping) // Java to Scala set conversion
    var taskNameToChangeLogPartitionMappingString = serializeTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMapping.map(kv => kv._1 -> kv._2.toInt).toMap)
    var envConfig = JsonConfigSerializer.toJson(config)
    val isCompressed = if (config.isEnvConfigCompressed) "TRUE" else "FALSE"

    if (config.isEnvConfigCompressed) {
      /**
       * If the compressed option is enabled in config, compress the 'ENV_CONFIG' and 'ENV_SYSTEM_STREAMS'
       * properties. Note: This is a temporary workaround to reduce the size of the config and hence size
       * of the environment variable(s) exported while starting a Samza container (SAMZA-337)
       */
      streamsAndPartsString = Util.compress(streamsAndPartsString)
      taskNameToChangeLogPartitionMappingString = Util.compress(taskNameToChangeLogPartitionMappingString)
      envConfig = Util.compress(envConfig)
    }

    Map(
      ShellCommandConfig.ENV_CONTAINER_NAME -> name,
      ShellCommandConfig.ENV_SYSTEM_STREAMS -> streamsAndPartsString,
      ShellCommandConfig.ENV_TASK_NAME_TO_CHANGELOG_PARTITION_MAPPING -> taskNameToChangeLogPartitionMappingString,
      ShellCommandConfig.ENV_CONFIG -> envConfig,
      ShellCommandConfig.ENV_JAVA_OPTS -> config.getTaskOpts.getOrElse(""),
      ShellCommandConfig.ENV_JAVA_HOME -> config.getJavaHome.getOrElse(""),
      ShellCommandConfig.ENV_COMPRESS_CONFIG -> isCompressed)

  }
}
