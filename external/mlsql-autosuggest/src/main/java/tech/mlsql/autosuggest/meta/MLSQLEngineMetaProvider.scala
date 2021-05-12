package tech.mlsql.autosuggest.meta

import java.nio.charset.Charset
import java.util.UUID

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.common.utils.serder.json.JSONTool


/**
  * 26/7/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLEngineMetaProvider() extends MetaProvider {
  override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    val form = Form.form()

    if (key.prefix.isEmpty) return None
    var path = ""

    if (key.db.isDefined) {
      path += key.db.get + "."
    }

    path += key.table
    val tableName = UUID.randomUUID().toString.replaceAll("-", "")

    var sql: String = ""

    val sqlStr: String = extra.getOrElse("sql", "None")
    val setStr: String = extra.getOrElse("set", "None")

    if (extra.size != 0 && !sqlStr.equals("None")) {
      sql =
        s"""
           |load ${key.prefix.get}.`${path}` where header="true" and directQuery = ${sqlStr} as ${tableName} ;!desc ${tableName};
           |""".stripMargin
    } else if (extra.size != 0 && !setStr.equals("None")) {
      sql =
        s"""
           | ${setStr}
           | load ${key.prefix.get}.`${path}` where header="true" as ${tableName};!desc ${tableName};
           |""".stripMargin

    } else {
      sql =
        s"""
           |load ${key.prefix.get}.`${path}` where header="true" as ${tableName};!desc ${tableName};
           |""".stripMargin
    }
    val params = JSONTool.parseJson[Map[String, String]](AutoSuggestContext.context().options("params"))
    params.foreach { case (k, v) =>
      if (k != "sql" && k != "executeMode") {
        form.add(k, v)
      }
    }
    form.add("sql", sql)
    val resp = Request.Post(params("schemaInferUrl")).bodyForm(form.build(), Charset.forName("utf8")).execute().returnResponse()
    if (resp.getStatusLine.getStatusCode == 200) {
      val str = EntityUtils.toString(resp.getEntity)
      val columns = JSONTool.parseJson[List[TableSchemaColumn]](str)
      var tableName = extra.getOrElse("table", "None")
      val table = if (!tableName.equals("None")) {
        MetaTable(key.copy(prefix = Option("temp table"), table = tableName), columns.map { item =>
          MetaTableColumn(item.col_name, item.data_type, true, Map())
        })
      } else {
        MetaTable(key, columns.map { item =>
          MetaTableColumn(item.col_name, item.data_type, true, Map())
        })
      }

      Option(table)
    } else None
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = {
    List()
  }
}

case class TableSchemaColumn(col_name: String, data_type: String, comment: Option[String])
