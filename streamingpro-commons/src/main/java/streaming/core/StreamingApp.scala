/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.core

import streaming.core.strategy.platform.PlatformManager
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.shell.command.ParamsUtil
import tech.mlsql.runtime.MLSQLPlatformLifecycle


object StreamingApp {

  def main(args: Array[String]): Unit = {
    //将主方法中的配置转换成map
    val params = new ParamsUtil(args)
    //这一步是检查上一步map是否转换成功，查看key中是否存在streaming.name,否则抛出异常
    require(params.hasParam("streaming.name"), "Application name should be set")
    //创建平台管理对象 PlatformManager
    val platform = PlatformManager.getOrCreate
    TryTool.tryOrExit {
      List("tech.mlsql.runtime.LogFileHook", "tech.mlsql.runtime.PluginHook").foreach { className =>
        //根据反射获取MlsqlPlatformLifecycle对象, 启动mlsql
        platform.registerMLSQLPlatformLifecycle(
          Class.forName(className).
            newInstance().asInstanceOf[MLSQLPlatformLifecycle])
      }

    }

    platform.run(params)
  }

}

class StreamingApp
