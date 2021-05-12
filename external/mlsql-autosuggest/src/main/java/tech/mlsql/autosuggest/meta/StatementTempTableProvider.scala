package tech.mlsql.autosuggest.meta

/**
  * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class StatementTempTableProvider extends MetaProvider {
  //存储临时表
  private val cache = scala.collection.mutable.HashMap[String, MetaTable]()

  //存储set变量集合
  private val setCache = scala.collection.mutable.HashMap[String, MetaTable]()

  //存储set变量语句（key, set语句）
  private val setKVCache = scala.collection.mutable.HashMap[String, String]()

  //存储load script加载的脚本名称，为了在ScriptUDF中提示
  private val scriptCache = scala.collection.mutable.HashMap[String, MetaTable]()

  //存储register中的函数名
  private val udfFunCache = scala.collection.mutable.HashMap[String, MetaTable]()

  //获取所有set变量
  def setList(extra: Map[String, String] = Map()): List[MetaTable] = {
    setCache.values.toList.filter(f => f.key.table != null)
  }

  //搜索set
  def setRegister(name: String, metaTable: MetaTable) = {
    setCache += (name -> metaTable)
  }

  //根据key获取value
  def setKVSearch(key: String): Option[String] = {
    setKVCache.get(key)
  }

  //存储set变量
  def setKVRegister(key: String, value: String) = {
    setKVCache += (key -> value)
  }

  //获取所有load加载的脚本名
  def scriptList(extra: Map[String, String] = Map()): List[MetaTable] = {
    scriptCache.values.toList.filter(f => f != null)
  }

  //存储脚本名
  def scriptRegister(key: String, value: MetaTable) = {
    scriptCache += (key -> value)
  }

  //获取所有register语句中函数名
  def udfFunList(extra: Map[String, String] = Map()): List[MetaTable] = {
    udfFunCache.values.toList.filter(f => f != null)
  }

  //存储register中的函数名
  def udfFunRegister(key: String, value: MetaTable) = {
    udfFunCache += (key -> value)
  }


  override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    cache.get(key.table)
  }

  def register(name: String, metaTable: MetaTable) = {
    cache += (name -> metaTable)
    this
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = cache.values.toList
}
