package streaming.dsl.mmlib.algs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import org.apache.http.client.fluent.{Request, Response}
import org.apache.http.entity.mime.MultipartEntityBuilder

/**
  * Created by Lenovo on 2021/1/28.
  */
class SQLDownloadLocalExt extends SQLAlg with Logging with WowLog with WowParams {

  override val uid: String = BaseParams.randomUID()

  def evaluate(value: String) = {
    def withPathPrefix(prefix: String, path: String): String = {

      val newPath = path
      if (prefix.isEmpty) return newPath

      if (path.contains("..")) {
        throw new RuntimeException("path should not contains ..")
      }
      if (path.startsWith("/")) {
        return prefix + path.substring(1, path.length)
      }
      return prefix + newPath

    }

    val context = ScriptSQLExec.context()
    withPathPrefix(context.home, value)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    params.get(from.name).map { s =>
      set(from, evaluate(s))
      s
    }.getOrElse {
      throw new MLSQLException(s"${from.name} is required")
    }
    val filePath = params(from.name)
    val fileName: String = filePath.substring(filePath.lastIndexOf("/") + 1)
    logInfo(format(s"download file from src:${$(from)}"))
    val context = ScriptSQLExec.context()
    val clientPath = context.userDefinedParam.get("__default__fileserver_url__") match {
      case Some(fileServer) => fileServer
      case None =>
        throw new MLSQLException(s"__default__fileserver_url__ is required")
    }

    var conf: Configuration = new Configuration();
    conf.addResource("core-site.xml")
    var fs: FileSystem = FileSystem.get(conf);
    //文件上传：create()方法，返回一个用来写入数据的输出流
    //文件下载：open()方法来打开某个给定文件的输入流
    var fsdi: FSDataInputStream = fs.open(new Path($(from)));
    //将流传给console端
    val uploadFile = Request.Post(clientPath)
    val builder: MultipartEntityBuilder = MultipartEntityBuilder.create
    builder.addBinaryBody(fileName, fsdi)
    uploadFile.body(builder.build())
    val response: Response = uploadFile.execute()

    df
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  final val from: Param[String] = new Param[String](this, "from", "the file(directory) name you have uploaded")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

}
