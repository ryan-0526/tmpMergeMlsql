package streaming.core.datasource.impl

import java.util.Properties

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.repository.SchemaRepository
import com.alibaba.druid.sql.visitor.SchemaStatVisitor
import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.catalyst.plans.logical.MLSQLDFParser
import org.apache.spark.sql.execution.WowTableIdentifier
import org.apache.spark.sql.mlsql.session.{MLSQLException, MLSQLSparkSession}
import org.apache.spark.sql._
import _root_.streaming.core.datasource._
import _root_.streaming.dsl.auth.{MLSQLTable, OperateType, TableType}
import _root_.streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import _root_.streaming.log.WowLog
import tech.mlsql.Stage
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.DatasourceAuth
import tech.mlsql.sql.{MLSQLSQLParser, MLSQLSparkConf}

import scala.collection.JavaConverters._

/**
  * 2018-12-21 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDirectJDBC extends MLSQLDirectSource with MLSQLDirectSink with MLSQLSourceInfo with MLSQLRegistry
  with DatasourceAuth with Logging with WowLog {

  override def fullFormat: String = "jdbc"

  override def shortFormat: String = fullFormat

  override def dbSplitter: String = "."

  def toSplit = "\\."

  private def loadConfigFromExternal(params: Map[String, String], path: String) = {
    var _params = params
    var isRealDB = true
    if (path.contains(".")) {
      val Array(db, table) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
        isRealDB = false
      })
    }
    _params
  }


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    var conParmas = Map[String,String]()
    var url = config.config.get("url")
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        reader.options(options)
        conParmas = options
        url = options.get("url")
      })
    }

    //load configs should overwrite connect configs
    reader.options(config.config)

    val dbtable = "(" + config.config("directQuery") + ") temp"

    if (config.config.contains("prePtnArray")) {
      val prePtn = config.config.get("prePtnArray").get
        .split(config.config.getOrElse("prePtnDelimiter", ","))

      reader.jdbc(url.get, dbtable, prePtn, new Properties())
    } else {
      reader.option("dbtable", dbtable)

      //-------------????????????
      def addSparkOption(option1: Array[Row]): Unit ={
        var maxValue = option1(0).get(0).toString
        var minValue = option1(0).get(1).toString
        reader.option("lowerBound" , minValue)
        reader.option("upperBound" , maxValue)
      }

      //-------------?????????????????????????????????????????????????????????????????????????????????
      val jdbcConnParmas: Map[String, String] = config.config
      if (jdbcConnParmas.contains("partitionColumn")){
        if (!(jdbcConnParmas.contains("lowerBound")) && !(jdbcConnParmas.contains("upperBound"))){
          val value: String = jdbcConnParmas.getOrElse("partitionColumn","null")
          val dfram1: Option[DataFrame] = config.df
          val sparkS: SparkSession = dfram1.get.sparkSession
          val frame: DataFrame = sparkS.read.options(conParmas).option("dbtable",dbtable).format(format).load()
          frame.describe()
          frame.createOrReplaceTempView("formatTable")

          //????????????????????????,????????????
          val scope: String = jdbcConnParmas.getOrElse("conditionExpr","null")
          val whereS: String = if(scope != "null") {" where " + scope} else {" "}
          val frame1: DataFrame = sparkS.sql(
            s"""
               |select
               |nvl(cast(max(${value}) as String), 0) as max_id,
               |nvl(cast(min(${value}) as String), 0) as min_id
               |from formatTable
               | ${whereS}
            """.stripMargin)
          val option1: Array[Row] = frame1.collect()
          //??????spark?????????????????????
          addSparkOption(option1)
        }
      }
      //-------------end
      reader.format(format).load()
    }
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new MLSQLException("not support yet....")
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLDirectDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLDirectDataSourceType), this)
  }

  override def unRegister(): Unit = {
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(fullFormat, MLSQLDirectDataSourceType))
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(shortFormat, MLSQLDirectDataSourceType))
  }

  def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    } else {
      Array("", config.path)
    }

    val url = if (config.config.contains("url")) {
      config.config.get("url").get
    } else {
      val format = config.config.getOrElse("implClass", fullFormat)

      ConnectMeta.options(DBMappingKey(format, _dbname)) match {
        case Some(item) => item("url")
        case None => throw new RuntimeException(
          s"""
             |format: ${format}
             |ref:${_dbname}
             |However ref is not found,
             |Have you set the connect statement properly?
           """.stripMargin)
      }
    }

    val dataSourceType = url.split(":")(1)
    val dbName = url.substring(url.lastIndexOf('/') + 1).takeWhile(_ != '?')
    val si = SourceInfo(dataSourceType, dbName, _dbtable)
    SourceTypeRegistry.register(dataSourceType, si)

    si
  }

  // this function depends on druid, so we can
  // fix chinese tablename since in spark parser it's not supported
  //MLSQLAuthParser.filterTables(sql, context.execListener.sparkSession)
  def extractTablesFromSQL(sql: String, dbType: String = JdbcConstants.MYSQL) = {
    val repository = new SchemaRepository(dbType)
    repository.console(sql)
    val stmt = SQLUtils.parseSingleStatement(sql, dbType)
    val visitor = new SchemaStatVisitor()
    stmt.accept(visitor)
    visitor.getTables().asScala.map { f =>
      val dbAndTable = f._1.getName
      if (dbAndTable.contains(".")) {
        val Array(db, table) = dbAndTable.split("\\.", 2)
        WowTableIdentifier(table, Option(db), None)
      } else WowTableIdentifier(dbAndTable, None, None)
    }.toList

  }

  override def auth(path: String, params: Map[String, String]): List[MLSQLTable] = {
    val si = this.sourceInfo(DataAuthConfig(path, params))

    val context = ScriptSQLExec.contextGetOrForTest()

    // we should auth all tables in direct query
    val sql = params("directQuery")
    val dbType = params.getOrElse("dbType", JdbcConstants.MYSQL)
    // first, only select supports
    if (!sql.trim.toLowerCase.startsWith("select")) {
      throw new MLSQLException("JDBC direct query only support select statement")
    }
    logInfo("Auth direct query tables.... ")

    //second, we should extract all tables from the sql
    val tableRefs = extractTablesFromSQL(sql, dbType)

    def isSkipAuth = {
      context.execListener.env()
        .getOrElse("SKIP_AUTH", "true")
        .toBoolean
    }

    tableRefs.foreach { tableIdentify =>
      if (tableIdentify.database.isDefined) {
        throw new MLSQLException("JDBC direct query should not allow using db prefix. Please just use table")
      }
    }

    if (!isSkipAuth && context.execListener.getStage.get == Stage.auth && !MLSQLSparkConf.runtimeDirectQueryAuth) {
      val _params = loadConfigFromExternal(params, path)
      val tableList = tableRefs.map(_.identifier)
      val tableColsMap = JDBCUtils.queryTableWithColumnsInDriver(_params, tableList)
      val createSqlList = JDBCUtils.tableColumnsToCreateSql(tableColsMap)
      val tableAndCols = MLSQLSQLParser.extractTableWithColumns(si.sourceType, sql, createSqlList)

      tableAndCols.map { tc =>
        MLSQLTable(Option(si.db), Option(tc._1), Option(tc._2.toSet), OperateType.DIRECT_QUERY, Option(si.sourceType), TableType.JDBC)
      }.foreach { mlsqlTable =>
        context.execListener.authProcessListner match {
          case Some(authProcessListener) =>
            authProcessListener.addTable(mlsqlTable)
          case None =>
        }
      }
    }

    if (!isSkipAuth && context.execListener.getStage.get == Stage.physical && MLSQLSparkConf.runtimeDirectQueryAuth) {
      val _params = loadConfigFromExternal(params, path)
      //clone new sparksession so we will not affect current spark context catalog
      val spark = MLSQLSparkSession.cloneSession(context.execListener.sparkSession)
      tableRefs.map { tableIdentify =>
        spark.read.options(_params + ("dbtable" -> tableIdentify.table)).format("jdbc").load().createOrReplaceTempView(tableIdentify.table)
      }

      val df = spark.sql(sql)
      val tableAndCols = MLSQLDFParser.extractTableWithColumns(df)
      var mlsqlTables = List.empty[MLSQLTable]

      tableAndCols.foreach {
        case (table, cols) =>
          mlsqlTables ::= MLSQLTable(Option(si.db), Option(table), Option(cols.toSet), OperateType.DIRECT_QUERY, Option(si.sourceType), TableType.JDBC)
      }
      context.execListener.getTableAuth.foreach { tableAuth =>
        println(mlsqlTables)
        tableAuth.auth(mlsqlTables)
      }

    }
    List()
  }
}
