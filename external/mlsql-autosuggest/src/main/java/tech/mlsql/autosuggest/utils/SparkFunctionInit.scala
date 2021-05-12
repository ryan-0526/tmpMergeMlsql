package tech.mlsql.autosuggest.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.autosuggest.MLSQLSQLFunction
import tech.mlsql.autosuggest.MLSQLSQLFunction.DB_KEY
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.dsl.adaptor.MLMapping

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Lenovo on 2021/4/8.
  */
object SparkFunctionInit {

  def init: Unit = {
    val sqlAlg = MLMapping.findAlg("ShowFunctionsExt")
    val session: SparkSession = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession
    val frame: DataFrame = sqlAlg.train(session.emptyDataFrame,"",Map())

    val list: List[String] = frame.collect().map(m => m.getAs[String](0)).toList

    var columns: ArrayBuffer[MetaTableColumn] = ArrayBuffer[MetaTableColumn]()

    list.map{
      case funName =>
        columns = columns .:+ (MetaTableColumn(funName,"",true,Map()))
        val funTable = MetaTable(MetaTableKey(None, Option(DB_KEY), funName), columns.toList)
        MLSQLSQLFunction.funcMetaProvider.register(funTable)
    }

  }

}
