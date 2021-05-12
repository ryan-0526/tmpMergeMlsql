package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableKey}

/**
  * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
object SpecialTableConst {
  val KEY_WORD = "__KEY__WORD__"
  val DATA_SOURCE_KEY = "__DATA__SOURCE__"
  val OPTION_KEY = "__OPTION__"
  val MODEL = "__MODEL__"
  val TEMP_TABLE_DB_KEY = "__TEMP_TABLE__"

  val OTHER_TABLE_KEY = "__OTHER__TABLE__"

  val TOP_LEVEL_KEY = "__TOP_LEVEL__"

  val PLUG = "__PLUG__"

  val TEMP_SET_TEXT_VAR = "__TEMP_SET_TEXT_VAR__"
  val TEMP_SET_SQL_VAR = "__TEMP_SET_SQL_VAR__"
  val TEMP_SET_SHELL_VAR = "__TEMP_SET_SHELL_VAR__"
  val TEMP_SET_DEFAULTPARAM_VAR = "__TEMP_SET_DEFAULTPARAM_VAR__"

  val UDF_FUNCTION = "__UDF_FUNCTION__"

  val PLUG_CONDITION = "__PLUG_CONDITION__"


  def KEY_WORD_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.KEY_WORD), List())

  def DATA_SOURCE_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.DATA_SOURCE_KEY), List())

  def OPTION_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.OPTION_KEY), List())

  def SAVE_MODEL = MetaTable(MetaTableKey(None, None, SpecialTableConst.MODEL), List())

  def PLUG_IN = MetaTable(MetaTableKey(None, None, SpecialTableConst.PLUG), List())

  def PLUG_WHERE = MetaTable(MetaTableKey(None, None, SpecialTableConst.PLUG_CONDITION), List())

  def OTHER_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.OTHER_TABLE_KEY), List())

  def tempTable(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), name), List())

  def setTextTempVAR(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_SET_TEXT_VAR), name), List())

  def setSqlTempVAR(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_SET_SQL_VAR), name), List())

  def setShellTempVAR(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_SET_SHELL_VAR), name), List())

  def setDefaultParamTempVAR(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_SET_DEFAULTPARAM_VAR), name), List())


  def udfFunction(name: String) = MetaTable(MetaTableKey(None, Option(UDF_FUNCTION), name), List())

  def plugCondition(name: String) = MetaTable(MetaTableKey(None, Option(PLUG_CONDITION), name), List())


  def subQueryAliasTable = {
    MetaTableKey(None, None, null)
  }
}
