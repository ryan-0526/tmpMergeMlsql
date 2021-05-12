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

package streaming.dsl.mmlib.algs.param

import java.lang.reflect.{AnnotatedType, Field}

import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by allwefantasy on 20/9/2018.
 */
trait WowParams extends Params {
  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  def _explainParams(sparkSession: SparkSession, f: () => Params) = {

    val name1: String = this.getClass.getCanonicalName
    val rfcParams2 = this.params.map(this.explainParam).map(f => Row.fromSeq(f.split(":", 2)))
    val model = f()
    val rfcParams = model.params.map(model.explainParam).map { f =>
      val Array(name, value) = f.split(":", 2)
      Row.fromSeq(Seq("fitParam.[group]." + name, value))
    }
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2 ++ rfcParams, 1), StructType(Seq(StructField("param", StringType), StructField("description", StringType))))
  }

  def _explainParams(sparkSession: SparkSession) = {
    /*val map: ParamMap = this.extractParamMap
    val name1: String = this.params.getClass.getCanonicalName
    val fields: Array[Field] = this.params.getClass.getDeclaredFields


    this.params.map(p => {
      val value: Param[Any] = this.getParam(p.name)
      val interfaces: Array[AnnotatedType] = value.getClass.getAnnotatedInterfaces
      value.
    })

    val name: String = this.params.getClass.getTypeName
    val strings: Array[String] = this.params.map(m => m.getClass.getCanonicalName)
    strings.foreach(println(_))
    val name2: String = new IntParam("fd","","").getClass.getCanonicalName
    val clazz: Class[_ <: String] = new IntParam("fd","","").getClass.getCanonicalName.getClass
    val params1: Array[Param[_]] = this.params
    val strings1: Array[String] = this.params.map(this.explainParam)*/

    val rfcParams2 = this.params.map(this.explainParam).map(f => Row.fromSeq(f.split(":", 2)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2, 1), StructType(Seq(StructField("param", StringType), StructField("description", StringType))))
  }

  def fetchParam[T](params: Map[String, String], param: Param[T], convert: (String) => T,
                    callback: Param[T] => Unit) = {
    params.get(param.name).map { item =>
      set(param, convert(item))
    }.getOrElse {
      callback(param)
    }
    $(param)
  }

  object ParamDefaultOption {
    def required[T](param: Param[T]): Unit = {
      throw new MLSQLException(s"${param.name} is required")
    }
  }

  object ParamConvertOption {
    def toInt(a: String): Int = {
      a.toInt
    }

    def nothing(a: String) = a
  }


}

object WowParams {
  def randomUID() = {
    Identifiable.randomUID(this.getClass.getName)
  }
}

