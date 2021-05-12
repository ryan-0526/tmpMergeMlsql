package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame
import streaming.dsl.load.batch.ModelParams
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos}
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.meta.MetaTable

/**
  * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
trait StatementUtils {

  def tokens: List[Token]

  def tokenPos: TokenPos

  def SPLIT_KEY_WORDS = {
    List(DSLSQLLexer.OPTIONS, DSLSQLLexer.WHERE, DSLSQLLexer.AS)
  }

  //用户提示`${}`
  def items = List[SuggestItem](SuggestItem("`${}`",SpecialTableConst.OTHER_TABLE, Map("desc" -> "reference set var")))

  def backAndFirstIs(t: Int, keywords: List[Int] = SPLIT_KEY_WORDS): Boolean = {
    // 从光标位置去找第一个核心词
    val temp = TokenMatcher(tokens, tokenPos.pos).back.orIndex(keywords.map(Food(None, _)).toArray)
    if (temp == -1) return false
    //第一个核心词必须是指定的词
    if (tokens(temp).getType == t) return true
    return false
  }

  //除去特殊符号(`${}`，"${}")，获取用户输入首字符
  def getTokenPrefix(tableName: String): String = {
    var tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos) //获取用户输入首字符 "`${xxx}"
    if (tokenPrefix.size >= 1 && "`".equals(tokenPrefix.head.toString)) {
      if (tokenPrefix.size >= 3 && ("`${".equals(tableName.substring(0, 3)) || "\"${".equals(tableName.substring(0, 3)))) {
        if (tokenPrefix.size > 3 && !("}".equals(tokenPrefix.substring(3, 4)))) {
          tokenPrefix = tokenPrefix.substring(3, 4)
        }
      } else {
        if (tokenPrefix.size >= 2 && !"`".equals(tableName.substring(1, 2))) {
          tokenPrefix = tokenPrefix.substring(1, 2)
        }
      }
    }

    tokenPrefix
  }

  //针对查询语句列中可以使用 "${" 判断用户要提示的临时表是set ,还是非set
  def getTempTable(context: AutoSuggestContext, usedCloumn: Boolean = true) = {
    val Tuple3(tableName, tokenPrefix, extraParam) = tokenPerfixAndExtra(context)
    val setList: List[MetaTable] = context.tempTableProvider.setList(extraParam) //获取set集合
    val allTables = setCacheList(tokenPrefix, context, setList) //获取所有set变量集合

    //判断用户输入字符的长度，如果长度符合，判断是否符合使用set变量的前缀
    if (usedCloumn && tableName.size >= 3 && ("`${".equals(tableName.substring(0, 3))
      || "\"${".equals(tableName.substring(0, 3)))) {
      LexerUtils.filterPrefixIfNeeded(allTables, tokens, tokenPos)

    } else if (!usedCloumn && tableName.size >= 3 && ("`${".equals(tableName.substring(0, 3)))) {
      LexerUtils.filterPrefixIfNeeded(allTables, tokens, tokenPos)

      //如果usedcloumn是false表示在 "${" 中不可以提示set变量
    } else if (!usedCloumn && tableName.size >= 3 && "\"${".equals(tableName.substring(0, 3))) {
      LexerUtils.filterPrefixIfNeeded(List(), tokens, tokenPos)

    } else {
      val tempTables: List[SuggestItem] = cacheList(context)
      LexerUtils.filterPrefixIfNeeded(tempTables ++ items, tokens, tokenPos)
    }
  }

  //针对plug.`` && plug.`${` 提示脚本名
  def getScriptTable(context: AutoSuggestContext): List[SuggestItem] = {
    val Tuple3(tableName, tokenPrefix, extraParam) = tokenPerfixAndExtra(context)
    //获取udf脚本名集合
    val udfList: List[MetaTable] = context.tempTableProvider.scriptList(extraParam)
    //获取set变量集合
    val setTables: List[MetaTable] = context.tempTableProvider.setList(extraParam)
    //val filterTables: List[MetaTable] = setTables.filter(f => f.key.table.startsWith(tokenPrefix))  //判断用户输入的首字符是否符合set集合中的表
    //不区分大小写
    val filterTables: List[MetaTable] = setTables.filter(f => StringUtils.startsWithIgnoreCase(f.key.table, tokenPrefix))

    if (tableName.size >= 2 && ("`".equals(tableName.head.toString) && "`".equals(tableName.last.toString))) {
      if (tableName.size >= 4 && ("${".equals(tableName.substring(1, 3)))) {
        //过滤掉临时表只留下set
        val setAllTable: List[SuggestItem] = notSetTables(fileterTempOrSetTable(udfList, filterTables, false))
        LexerUtils.backQuoteNeeded(setAllTable, tokens, tokenPos)
      } else {
        //过滤掉set表只留下临时表
        val scriptAllTable: List[SuggestItem] = notSetTables(fileterTempOrSetTable(udfList, filterTables))
        LexerUtils.backQuoteNeeded(scriptAllTable, tokens, tokenPos)
      }
    } else {
      List()
    }
  }


  //如果用户输入`${}`后所有set集合
  def setCacheList(tokenPrefix: String, context: AutoSuggestContext, setList: List[MetaTable]) = {
    val allTables = setList.map { item =>
      val prefix = (item.key.prefix, item.key.db) match {
        case (Some(prefix), Some(db)) => prefix
        case (Some(prefix), None) => prefix
        case (None, Some(SpecialTableConst.TEMP_SET_TEXT_VAR)) => "set text"
        case (None, Some(SpecialTableConst.TEMP_SET_SQL_VAR)) => "set sql"
        case (None, Some(SpecialTableConst.TEMP_SET_SHELL_VAR)) => "set shell"
        case (None, Some(SpecialTableConst.TEMP_SET_DEFAULTPARAM_VAR)) => "set defaultParam"
        case (None, Some(db)) => db
      }
      SuggestItem(item.key.table, item, Map("desc" -> prefix))
    }
    allTables
  }


  //解决不写`${}`也提示set变量
  def cacheList(context: AutoSuggestContext): List[SuggestItem] = {
    val Tuple3(tableName, tokenPrefix, extraParam) = tokenPerfixAndExtra(context)
    val setTables: List[MetaTable] = context.tempTableProvider.setList(extraParam) //获取set所有临时表
    //val filterTables: List[MetaTable] = setTables.filter(f => f.key.table.startsWith(tokenPrefix))  //判断用户输入的首字符是否符合set集合中的表
    val filterTables: List[MetaTable] = setTables.filter(f => StringUtils.startsWithIgnoreCase(f.key.table, tokenPrefix)) //不区分大小写过滤
    val tempTable: List[MetaTable] = context.metaProvider.list(extraParam) //获取所有已经生成的临时表
    //过滤from后不写`${}` 也能提示set表名问题
    val dropBackList: List[MetaTable] = fileterTempOrSetTable(tempTable, filterTables)
    notSetTables(dropBackList)
  }

  //不提示set变量集合
  def notSetTables(notSetList: List[MetaTable]) = {
    val allTables = notSetList.map { item =>
      val prefix = (item.key.prefix, item.key.db) match {
        case (Some(prefix), Some(db)) => prefix
        case (Some(prefix), None) => prefix
        case (None, Some(SpecialTableConst.TEMP_TABLE_DB_KEY)) => "temp table"
        case (None, Some(db)) => db
      }
      SuggestItem(item.key.table, item, Map("desc" -> prefix))
    }
    allTables
  }

  //boolean == ture 表示需要过滤set变量， == false ,表示需要过滤临时表，证明用户输入了`${`，想要提示set中的变量
  def fileterTempOrSetTable(tempTable: List[MetaTable], filterTables: List[MetaTable], boolean: Boolean = true): List[MetaTable] = {
    var dropBackList: List[MetaTable] = List()
    for (a <- 0 until tempTable.size) {
      var i = -1
      for (b <- 0 until filterTables.size) {

        if (i == -1 && (tempTable(a).key.table.equals(filterTables(b).key.table))) {
          i = 1
        }
      }
      if (boolean && i == -1) {
        dropBackList = dropBackList :+ tempTable(a)
      } else if (!boolean && i == 1) {
        dropBackList = dropBackList :+ tempTable(a)
      }
    }
    dropBackList
  }


  def tokenPerfixAndExtra(context: AutoSuggestContext) = {
    val tableName: String = tokens(tokenPos.pos).getText
    var tokenPrefix: String = getTokenPrefix(tableName) //获取用户输入前缀
    if (tokenPrefix.equals("`") || "`${".equals(tokenPrefix)) {
      tokenPrefix = ""
    }
    val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "") //获取用户名
    val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
    (tableName, tokenPrefix, extraParam)
  }


  def getPlugParam(plugName: String, context: AutoSuggestContext) = {
    val params = new ModelParams("modelParams", plugName, Map())(context.session)
    val table: DataFrame = params.explain
    var list: List[String] = List()
    table.collect.map(c => {
      val str: String = c.getAs[String](0)
      list = list :+ str
    })

    list.map { m =>
      SuggestItem(m, SpecialTableConst.PLUG_WHERE, Map("desc" -> "condition"))
    }

    val colList: List[SuggestItem] = list.map { col =>
      SuggestItem(col, SpecialTableConst.PLUG_WHERE, Map("desc" -> "condition"))
    }

    LexerUtils.filterPrefixIfNeeded(colList, tokens, tokenPos)
  }


  /*def getTokenPrefix(tableName:String):String = {
    var tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos)   //获取用户输入首字符 "`${xxx}"
    if (tokenPrefix.size >= 3) {
      val fPrefixBool: Boolean = "`${".equals(tableName.substring(0,3))
      val fxPrefixBool: Boolean = "\"${".equals(tableName.substring(0,3))
      if (tokenPrefix.size > 3 && (fPrefixBool || fxPrefixBool)) {
        tokenPrefix = tokenPrefix.substring(3,4)
      }
    }
    tokenPrefix
  }

  def setCacheList(tokenPrefix: String,context: AutoSuggestContext) = {
    val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "")
    val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
    val allTables = context.tempTableProvider.setList(extraParam).map { item =>
      val prefix = (item.key.prefix, item.key.db) match {
        case (Some(prefix), Some(db)) => prefix
        case (Some(prefix), None) => prefix
        case (None, Some(SpecialTableConst.TEMP_SET_TEXT_VAR)) => "set text"
        case (None, Some(SpecialTableConst.TEMP_SET_SQL_VAR)) => "set sql"
        case (None, Some(SpecialTableConst.TEMP_SET_SHELL_VAR)) => "set shell"
        case (None, Some(SpecialTableConst.TEMP_SET_DEFAULTPARAM_VAR)) => "set defaultParam"
        case (None, Some(db)) => db
      }
      SuggestItem(item.key.table, item, Map("desc" -> prefix))
    }
    allTables
  }

  //判断用户要提示的临时表是set ,还是非set
  def getTempTable(context: AutoSuggestContext) = {
    val tableName: String = tokens(tokenPos.pos).getText
    //判断用户输入字符的长度，如果长度符合，判断是否符合使用set变量的前缀
    if (tableName.size >= 3 && ("`${".equals(tableName.substring(0,3)) || "\"${".equals(tableName.substring(0,3)))) {
      val tokenPrefix: String = getTokenPrefix(tableName) //获取用户输入前缀
      val allTables = setCacheList(tokenPrefix,context) //获取所有set变量集合
      LexerUtils.filterPrefixIfNeeded(allTables, tokens, tokenPos)

    } else {
      val tempTables: List[SuggestItem] = cacheList(context)
      LexerUtils.filterPrefixIfNeeded(tempTables, tokens, tokenPos)
    }
  }


  def cacheList(context: AutoSuggestContext) : List[SuggestItem] = {
    val tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos)   //获取用户输入首字符
    val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "")
    val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
    val setTables: List[MetaTable] = context.tempTableProvider.setList(extraParam)  //获取set所有临时表
    val filterTables: List[MetaTable] = setTables.filter(f => f.key.table.startsWith(tokenPrefix))  //判断用户输入的首字符是否符合set集合中的表
    val tempTable: List[MetaTable] = context.metaProvider.list(extraParam) //获取所有已经生成的临时表
    //过滤from后不写`${}` 也能提示set表名问题
    val dropBackList: List[MetaTable] = dropTempTable(tempTable,filterTables)
    val allTables = dropBackList.map { item =>
      val prefix = (item.key.prefix, item.key.db) match {
        case (Some(prefix), Some(db)) => prefix
        case (Some(prefix), None) => prefix
        case (None, Some(SpecialTableConst.TEMP_TABLE_DB_KEY)) => "temp table"
        case (None, Some(db)) => db
      }
      SuggestItem(item.key.table, item, Map("desc" -> prefix))
    }
    allTables
  }

  //删除临时表中的set表
  def dropTempTable(tempTable:List[MetaTable],filterTables:List[MetaTable]):List[MetaTable] = {
    var dropBackList:List[MetaTable] = List()
    for( a <- 0 until tempTable.size){
      var i = -1
      for( b <- 0 until filterTables.size){
        //如果两个集合中的table不同，存入新集合
        if (i == -1 && (tempTable(a).key.table.equals(filterTables(b).key.table))){
          i = 1
        }
      }
      if (i == -1) {
        dropBackList = dropBackList :+ tempTable(a)
      }
    }
    dropBackList
  }*/

}

