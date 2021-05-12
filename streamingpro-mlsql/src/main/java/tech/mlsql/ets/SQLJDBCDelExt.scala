package tech.mlsql.ets

import java.sql.{Connection, ResultSet, SQLException, Statement}

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import tech.mlsql.job.JobManager

class SQLJDBCDelExt(override val uid: String) extends SQLAlg with WowParams{
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]):DataFrame={
    params.get(keyCol.name).
      map(m => set(keyCol, m)).getOrElse {
      throw new MLSQLException("keyCol is required")
    }
    val Array(db, table) = path.split("\\.", 2)
    var _params = params
    if (path.contains(".")) {
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
      })
    }
    val keyColumns = params.get(keyCol.name).getOrElse("").toUpperCase.split(",")
    var sql = s"delete from $table where "
    var flag_control = true
    var whereTemp = ""
    keyColumns.foreach(x =>{
      if( df.schema.filter(p => p.name.toLowerCase==x.toLowerCase).length<1) {
        throw new IllegalArgumentException(
          s"Can't find delete field value:$x"
        )
      }
      if(flag_control){
        whereTemp=" "+x+"=?"
        flag_control = false
      }
      else {
        whereTemp+=" and "+x+"=?"
      }
    })

    sql = sql+whereTemp
    var targetRDD = df.sparkSession.read
      .format(_params("format"))
      .option("url",_params("url"))
      .option("driver",_params("driver"))
      .option("user",_params("user"))
      .option("password",_params("password"))
      .option("dbtable", table).load()
    var columnsSchema=targetRDD.schema.fields.toList

    df.foreachPartition(f => {
      val connection = ConnectDB(_params)
      val stmt = connection.prepareStatement(sql)
      connection.setAutoCommit(false)
      while (f.hasNext) {
        val row = f.next()
        var order = 0;

        for (column <- keyColumns) {
          order = order + 1
          var columnSchema = columnsSchema.filter(p => p.name.toLowerCase==column.toLowerCase)
          if(columnSchema.length<1)
          {
            throw new IllegalArgumentException(
              "target table Can't find field:"+column
            )
          }
          columnSchema(0).dataType match {
            case IntegerType => stmt.setInt(order, row.getAs[Int](column))
            case LongType => stmt.setLong(order, row.getAs[Long](column))
            case DoubleType => stmt.setDouble(order, row.getAs[Double](column))
            case FloatType => stmt.setFloat(order, row.getAs[Float](column))
            case ShortType => stmt.setInt(order, row.getAs[Short](column))
            case ByteType => stmt.setInt(order, row.getAs[Byte](column))
            case BooleanType => stmt.setBoolean(order, row.getAs[Boolean](column))
            case StringType => stmt.setString(order, row.getAs[String](column))
            case BinaryType => stmt.setBytes(order, row.getAs[Array[Byte]](column))
            case TimestampType => stmt.setTimestamp(order, row.getAs[java.sql.Timestamp](column))
            case DateType => stmt.setDate(order,row.getAs[java.sql.Date](column))
            case t: DecimalType => stmt.setBigDecimal(order,row.getAs[java.math.BigDecimal](column))
            case _ => throw new IllegalArgumentException(
              "Can't translate non-null value for field "+columnSchema(0).dataType
            )
          }
        }
        stmt.addBatch()
      }
      try{
        stmt.executeBatch()
        connection.commit()
      }catch{
        case ex: SQLException => {
          connection.rollback()
          ex.printStackTrace()
          throw new SQLException("Delete Exception")
        }
      } finally {
        stmt.close()
        connection.close()
      }
    })

    import df.sparkSession.implicits._
    val context = ScriptSQLExec.context()
    val job = JobManager.getJobInfo(context.groupId)
    df.sparkSession.createDataset(Seq(job)).toDF()

  }

  def ConnectDB(options: Map[String, String]):Connection = {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
    connection
  }

  def getBack(options: Map[String, String],sql:String,flag:String):String = {
    var connection:Connection = null
    var statement:Statement = null
    var rs:ResultSet = null
    try{
      connection = ConnectDB(options)
      statement = connection.createStatement()
      rs = statement.executeQuery(sql)
      var template = ""
      while (rs.next()){
        flag match {
          case "1" => template=rs.getString("template")
          case  "2" => template=template+"||"+rs.getString("type")+rs.getString("total")
          case _ =>
        }
      }
      template
    }catch{
      case ex: SQLException => {
        throw new SQLException("CheckDataExt Exception")
      }
    } finally {
      statement.close()
      rs.close()
      connection.close()
    }
  }

  override def skipPathPrefix: Boolean = true
  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = throw new RuntimeException(s"${getClass.getName} not support load function.")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]):UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }
  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)
  final val keyCol: Param[String] = new Param[String](this, "keyCol", "")
}
