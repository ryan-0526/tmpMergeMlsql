package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos}

import scala.collection.mutable

/**
  * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class SelectSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()
  register(classOf[ProjectSuggester])
  register(classOf[FromSuggester])
  register(classOf[FilterSuggester])
  register(classOf[JoinSuggester])
  register(classOf[JoinOnSuggester])
  register(classOf[OrderSuggester])
  register(classOf[BySuggester])
  register(classOf[HavingSuggester])

  override def name: String = "select"

  private lazy val newTokens = LexerUtils.toRawSQLTokens(context, _tokens) //获取tokens
  private lazy val TABLE_INFO = mutable.HashMap[Int, mutable.HashMap[MetaTableKeyWrapper, MetaTable]]()
  private lazy val selectTree: SingleStatementAST = buildTree()

  def sqlAST = selectTree

  def tokens = newTokens

  def table_info = TABLE_INFO

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SELECT) => true
      case _ => false
    }
  }

  private def buildTree() = {
    val root = SingleStatementAST.build(this, newTokens)
    import scala.collection.mutable

    //如果有子查询，会先获取子查询信息
    root.visitUp(level = 0) { case (ast: SingleStatementAST, level: Int) =>
      if (!TABLE_INFO.contains(level)) {
        TABLE_INFO.put(level, new mutable.HashMap[MetaTableKeyWrapper, MetaTable]())
      }

      if (level != 0 && !TABLE_INFO.contains(level - 1)) {
        TABLE_INFO.put(level - 1, new mutable.HashMap[MetaTableKeyWrapper, MetaTable]())
      }


      //解析sql,如果找到核心词，将核心词后的字符(表名)跟已经生成临时表的表名进行匹配，如果匹配成功责将信息添加到TABLE_INFO集合中
      ast.tables(newTokens).foreach { item =>
        if (item.aliasName.isEmpty || item.metaTableKey != MetaTableKey(None, None, null)) {
          context.metaProvider.search(item.metaTableKey) match {
            case Some(res) =>
              TABLE_INFO(level) += (item -> res)
            case None =>
          }
        }
      }

      //判断子查询后是否有别名，如果有，则获取子查询信息
      val nameOpt = ast.name(newTokens)
      if (nameOpt.isDefined) {

        val metaTableKey = MetaTableKey(None, None, null)
        val metaTableKeyWrapper = MetaTableKeyWrapper(metaTableKey, nameOpt)
        val metaColumns = ast.output(newTokens).map { attr =>
          MetaTableColumn(attr, null, true, Map())
        }
        TABLE_INFO(level - 1) += (metaTableKeyWrapper -> MetaTable(metaTableKey, metaColumns))
        //TABLE_INFO .+ (level + 1 -> (metaTableKeyWrapper -> MetaTable(metaTableKey, metaColumns)))
      }

    }

    if (context.isInDebugMode) {
      logInfo(s"SQL[${newTokens.map(_.getText).mkString(" ")}]")
      logInfo(s"STRUCTURE: \n")
      TABLE_INFO.foreach { item =>
        logInfo(s"Level:${item._1}")
        item._2.foreach { table =>
          logInfo(s"${table._1} => ${table._2}")
        }
      }

    }

    root
  }

  override def suggest(): List[SuggestItem] = {
    var instance: StatementSuggester = null
    subInstances.foreach { _instance =>
      if (instance == null && _instance._2.isMatch()) {
        instance = _instance._2
      }
    }
    if (instance == null) List()
    else instance.suggest()

  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[SelectSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}


class ProjectSuggester(_selectSuggester: SelectSuggester) extends StatementSuggester with SelectStatementUtils with SuggesterRegister {

  def tokens = _selectSuggester.tokens

  def tokenPos = _selectSuggester.tokenPos

  def selectSuggester = _selectSuggester

  //提示 `${}`
  def items = List[SuggestItem](SuggestItem("`${}`",SpecialTableConst.OTHER_TABLE, Map("desc" -> "reference set var")))

  def doubleQuta = List[SuggestItem](SuggestItem("\"${}\"",SpecialTableConst.OTHER_TABLE, Map("desc" -> "reference set var")))

  def backAndFirstIs(t: Int, keywords: List[Int] = TokenMatcher.SQL_SPLITTER_KEY_WORDS): Boolean = {
    // 能找得到所在的子查询（也可以是最外层）
    val ast = getASTFromTokenPos //解析正在编写的sql
    if (ast.isEmpty) return false

    // 从光标位置去找第一个核心词
    val temp = TokenMatcher(tokens, tokenPos.pos).back.orIndex(keywords.map(Food(None, _)).toArray)
    if (temp == -1) return false
    //第一个核心词必须是是定的词，并且在子查询里，，根据第一个核心词找对应类
    if (tokens(temp).getType == t && temp >= ast.get.start && temp < ast.get.stop) return true
    return false
  }

  //修改三：让join也支持表名提示，获取所有临时表
  def cacheList: List[SuggestItem] = {
    val tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos) //获取用户输入首字符
    val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "")
    val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
    val setTables: List[MetaTable] = _selectSuggester.context.tempTableProvider.setList(extraParam) //获取set所有临时表
    val filterTables: List[MetaTable] = setTables.filter(f => f.key.table.startsWith(tokenPrefix)) //判断用户输入的首字符是否符合set集合中的表
    val tempTable: List[MetaTable] = _selectSuggester.context.metaProvider.list(extraParam) //获取所有已经生成的临时表

    //过滤from后不写`${}` 也能提示set表名问题
    var dropBackList: List[MetaTable] = dropTempTable(tempTable, filterTables)

    //删除本条sql中的表
    val wrappers: List[MetaTableKeyWrapper] = selectSuggester.sqlAST.tables(tokens)
    val tables: List[MetaTable] = wrappers.map(f =>
      MetaTable(MetaTableKey(f.metaTableKey.prefix, f.metaTableKey.db, f.aliasName.getOrElse(f.metaTableKey.table)),List()))

    dropBackList = dropTempTable(dropBackList,tables)

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

  //修改：让join, from ，as 后表名提示都支持set变量
  def setCacheList(tokenPrefix: String) = {
    val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "")
    val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
    val allTables = _selectSuggester.context.tempTableProvider.setList(extraParam).map { item =>
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

  //获取set语法中出去特殊字符"`${"后，用户输入的第一个字符
  def getTokenPrefix(tableName: String): String = {
    var tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos) //获取用户输入首字符 "`${xxx}"
    if (tokenPrefix.size >= 3) {
      val fPrefixBool: Boolean = "`${".equals(tableName.substring(0, 3))
      val fxPrefixBool: Boolean = "\"${".equals(tableName.substring(0, 3))
      if (tokenPrefix.size > 3 && (fPrefixBool || fxPrefixBool)) {
        tokenPrefix = tokenPrefix.substring(3, 4)
      }
    }
    tokenPrefix
  }

  //删除临时表中的set表
  def dropTempTable(tempTable: List[MetaTable], filterTables: List[MetaTable]): List[MetaTable] = {
    var dropBackList: List[MetaTable] = List()
    for (a <- 0 until tempTable.size) {
      var i = -1
      for (b <- 0 until filterTables.size) {
        //如果两个集合中的table不同，存入新集合
        if (i == -1 && (tempTable(a).key.table.equals(filterTables(b).key.table))) {
          i = 1
        }
      }
      if (i == -1) {
        dropBackList = dropBackList :+ tempTable(a)
      }
    }
    dropBackList
  }


  override def name: String = "project"

  override def isMatch(): Boolean = {
    val temp = backAndFirstIs(SqlBaseLexer.SELECT)
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"${name} is matched")
    }
    temp
  }

  override def suggest(): List[SuggestItem] = {
    val tableName: String = tokens(tokenPos.pos).getText
    //判断用户输入字符的长度，如果长度符合，判断是否符合使用set变量的前缀
    if (tableName.size >= 3 && ("`${".equals(tableName.substring(0, 3)) || "\"${".equals(tableName.substring(0, 3)))) {
      val tokenPrefix: String = getTokenPrefix(tableName)
      val allTables = setCacheList(tokenPrefix)
      LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ allTables, tokens, tokenPos)
    } else {
      LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ doubleQuta ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
    }
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class FilterSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {


  override def name: String = "filter"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.WHERE)

  }

  override def suggest(): List[SuggestItem] = {
    //val allTables: List[SuggestItem] = cacheList
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class JoinOnSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "join_on"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.ON)
  }

  override def suggest(): List[SuggestItem] = {
    //val allTables: List[SuggestItem] = cacheList
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }
}

class JoinSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "join"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.JOIN)
  }

  override def suggest(): List[SuggestItem] = {
    val tableName: String = tokens(tokenPos.pos).getText
    //判断用户输入字符的长度，如果长度符合，判断是否符合使用set变量的前缀
    if (tableName.size >= 3 && ("`${".equals(tableName.substring(0, 3)) || "\"${".equals(tableName.substring(0, 3)))) {
      val tokenPrefix: String = getTokenPrefix(tableName)
      val allTables = setCacheList(tokenPrefix)
      LexerUtils.filterPrefixIfNeeded(allTables, tokens, tokenPos)
    } else {
      //修改，让join支持表名提示
      val allTables: List[SuggestItem] = cacheList
      LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ items ++ allTables, tokens, tokenPos)
    }
  }
}

class FromSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "from"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.FROM)
  }

  override def suggest(): List[SuggestItem] = {
    val tableName: String = tokens(tokenPos.pos).getText
    //判断用户输入字符的长度，如果长度符合，判断是否符合使用set变量的前缀
    if (tableName.size >= 3 && ("`${".equals(tableName.substring(0, 3)) || "\"${".equals(tableName.substring(0, 3)))) {
      val tokenPrefix: String = getTokenPrefix(tableName)
      val allTables = setCacheList(tokenPrefix)
      LexerUtils.filterPrefixIfNeeded(allTables, tokens, tokenPos)
    } else {
      val allTables: List[SuggestItem] = cacheList
      LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ items ++ allTables, tokens, tokenPos)
    }
  }
}

class OrderSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "order"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.ORDER)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

}

class BySuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "by"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.BY)
  }

  override def suggest(): List[SuggestItem] = {
    val allTables: List[SuggestItem] = cacheList
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }
}

class HavingSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "having"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.HAVING)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(functionSuggest(), tokens, tokenPos)
  }
}










