package tech.mlsql.tool

import java.lang.Exception
import java.lang.reflect.Constructor

import streaming.dsl.mmlib.{ModelType, SQLAlg}
import streaming.dsl.mmlib.algs.Functions
import tech.mlsql.common.utils.reflect.ClassPath
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.ets.register.ETRegister

import scala.collection.mutable
import scala.util.control.Exception

/**
  * Created by Lenovo on 2021/3/29.
  */
class PlugClassify extends PlugGather{
  private val algorithmMap = new mutable.HashMap[String, String]() // 算法

  private val featurengineerMap = new mutable.HashMap[String, String]() //特征工程

  private val deepLearningMap = new mutable.HashMap[String, String]() //深度学习

  private val baseMap = new mutable.HashMap[String, String]() //基础插件


  private val sqlMap = new mutable.HashMap[String,String]() //sql开头插件 (基础插件)


  override def getAlgorithmMap = algorithmMap

  override def getFeaturengineerMap = featurengineerMap

  override def getDeepLearningMap = deepLearningMap

  override def getBaseMap = baseMap

  override def getSqlMap = sqlMap



  override def getSqlPlug(): Unit = {
    val funcRegs = ClassPath.from(classOf[Functions].getClassLoader).getTopLevelClasses("streaming.dsl.mmlib.algs").iterator()
    while (funcRegs.hasNext) {
      val wow = funcRegs.next()

      //val bool: Boolean = SQLAlg.getClass.isAssignableFrom(wow.load())

      val constructors: Array[Constructor[_]] = wow.load().getConstructors
      val array: Array[Constructor[_]] = constructors.filter(p => p.getParameterCount == 0)
      array.foreach(f => {
        if (f.newInstance().isInstanceOf[SQLAlg]) {
          val plugName = wow.load().newInstance().asInstanceOf[SQLAlg]
          sqlRegister(plugName.getClass.getSimpleName,plugName.getClass.getName)
        }
      })

    }
  }

  override def getAlgsPlug():Unit = {
    val algsMap: Map[String, String] = sqlMap.toMap ++ MLMapping.mapping ++ ETRegister.getMapping
    algsMap.map {
      case (key,value) =>
        var bool: Boolean = true
        try {
          var value1= Class.forName(value)
          bool = true
        } catch {
          case e: Exception =>
            bool = false
        }
        if (bool) {
          val plugName: SQLAlg = Class.forName(value).newInstance().asInstanceOf[SQLAlg]
          val modelType: ModelType = plugName.modelType
          if(modelType.name.equals("algType")) {
            val name: String = plugName.getClass.getSimpleName
            if (name.startsWith("SQL")) {
              algorRegister(name.substring(3,name.length),plugName.getClass.getName)
            } else {
              algorRegister(name,plugName.getClass.getName)
            }

          } else if (modelType.name.equals("processType")) {
            val name: String = plugName.getClass.getSimpleName
            if (name.startsWith("SQL")) {
              featureRegister(name.substring(3,name.length),plugName.getClass.getName)
            } else {
              featureRegister(name,plugName.getClass.getName)
            }

          } else if (modelType.name.equals("deepType")) {
            val name: String = plugName.getClass.getSimpleName
            if (name.startsWith("SQL")) {
              deepRegister(name.substring(3,name.length),plugName.getClass.getName)
            } else {
              deepRegister(name,plugName.getClass.getName)
            }

          } else if (modelType.name.equals("baseType")) {
            val name: String = plugName.getClass.getSimpleName
            if (name.startsWith("SQL")) {
              baseRegister(name.substring(3,name.length),plugName.getClass.getName)
            } else {
              baseRegister(name,plugName.getClass.getName)
            }

          }
        }
    }
  }


  def algorRegister(key:String,value:String) = {
    algorithmMap += (key -> value)
  }

  def featureRegister(key:String,value:String) = {
    featurengineerMap += (key -> value)
  }

  def deepRegister(key:String,value:String) = {
    deepLearningMap += (key -> value)
  }

  def baseRegister(key:String,value:String) = {
    baseMap += (key -> value)
  }


  def sqlRegister(key:String, value:String) = {
    sqlMap += (key -> value)
  }

}



