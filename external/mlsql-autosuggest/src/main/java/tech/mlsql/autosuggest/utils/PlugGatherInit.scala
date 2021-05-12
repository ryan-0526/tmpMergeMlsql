package tech.mlsql.autosuggest.utils

import tech.mlsql.tool.PlugGather

/**
  * Created by Lenovo on 2021/4/1.
  */
object PlugGatherInit {

  private var plugGather:PlugGather = _

  def init = {
    plugGather = Class.forName("tech.mlsql.tool.PlugClassify").newInstance().asInstanceOf[PlugGather]
    plugGather.getSqlPlug()
    plugGather.getAlgsPlug()
  }

  def getAlgorithmMap = plugGather.getAlgorithmMap()

  def getFeaturengineerMap = plugGather.getFeaturengineerMap

  def getDeepLearningMap = plugGather.getDeepLearningMap

  def getBaseMap = plugGather.getBaseMap

  def getSqlMap = plugGather.getSqlMap

}
