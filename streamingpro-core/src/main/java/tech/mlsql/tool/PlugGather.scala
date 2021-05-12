package tech.mlsql.tool

import scala.collection.mutable

/**
  * Created by Lenovo on 2021/3/29.
  */
trait PlugGather {
  def getSqlPlug(): Unit

  def getAlgsPlug(): Unit

  def getAlgorithmMap():mutable.HashMap[String,String]

  def getFeaturengineerMap :mutable.HashMap[String,String]

  def getDeepLearningMap :mutable.HashMap[String,String]

  def getBaseMap :mutable.HashMap[String,String]

  def getSqlMap :mutable.HashMap[String,String]
}
