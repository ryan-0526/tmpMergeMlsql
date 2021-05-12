package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.{BaseType, ModelType, SQLAlg}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

class SQLAutoIncrementKeyExt(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]):DataFrame ={
    val maxID = params.get(maxId.name).getOrElse("0")
    val isOrderT = params.get(isOrder.name)
    val schema:StructType = df.schema.add(StructField("SQLAutoIncrementKeyExt_id",LongType))
    val dfRDD: RDD[(Row, Long)] = df.rdd.zipWithIndex()
    val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
    var result = df.sparkSession.createDataFrame(rowRDD, schema)
    result = result.withColumn("id",result("SQLAutoIncrementKeyExt_id").cast(LongType)+maxID)

    isOrderT match {
      case Some(_) =>
        result = result.withColumn(isOrderT.getOrElse("no order columns"),result("SQLAutoIncrementKeyExt_id").cast(LongType)+maxID)
      case _   => result
    }
    result.drop("SQLAutoIncrementKeyExt_id")
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]):Any= throw new RuntimeException(s"${getClass.getName} not support load function.")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]):UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }
  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def modelType: ModelType = BaseType

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  final val maxId: Param[String] = new Param[String](this, "maxId", "")
  final val isOrder: Param[String] = new Param[String](this, "isOrder", "")
}