package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.autosuggest._
import tech.mlsql.autosuggest.meta.MetaTable
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable


/**
  * 8/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
trait SelectStatementUtils extends Logging {
  def selectSuggester: SelectSuggester

  def tokenPos: TokenPos

  def tokens: List[Token]


  def levelFromTokenPos = {
    var targetLevel = 0
    selectSuggester.sqlAST.visitDown(0) { case (ast, _level) =>
      if (tokenPos.pos >= ast.start && tokenPos.pos < ast.stop) targetLevel = _level
    }
    targetLevel
  }

  def getASTFromTokenPos: Option[SingleStatementAST] = {
    var targetAst: Option[SingleStatementAST] = None
    selectSuggester.sqlAST.visitUp(0) { case (ast, level) =>
      if (targetAst == None && (ast.start <= tokenPos.pos && tokenPos.pos < ast.stop)) {
        targetAst = Option(ast)
      }
    }
    targetAst
  }

  def table_info = {
    selectSuggester.table_info.get(levelFromTokenPos)
  }

  /*def table_info1 = {
    selectSuggester.table_info.values
  }*/


  def tableSuggest(): List[SuggestItem] = {
    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build

    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"tableSuggest, table_info:\n")
      logInfo("========tableSuggest start=======")
      if (table_info.isDefined) {
        table_info.get.foreach { case (key, metaTable) =>
          logInfo(key.toString + s"=>\n    key: ${metaTable.key} \n    columns: ${metaTable.columns.map(_.name).mkString(",")}")
        }
      }

      logInfo(s"Final: suggest ${!temp.isSuccess}")

      logInfo("========tableSuggest end =======")
    }

    if (temp.isSuccess) return List()


    def backAndFirstIs(t: Int, keywords: List[Int] = TokenMatcher.SQL_SPLITTER_KEY_WORDS): Boolean = {
      // 从光标位置去找第一个核心词
      val temp = TokenMatcher(tokens, tokenPos.pos).back.orIndex(keywords.map(Food(None, _)).toArray)
      if (temp == -1) return false
      //第一个核心词必须是指定的词
      if (tokens(temp).getType == t) return true
      return false
    }


    table_info match {
      //区分表名和别名问题，包括别名和`${}`中的表名一样问题，怎么区分
      case Some(tb) =>
        val suggestList = tb.map {
          case (metaTableKeyWrapper, metaTable) =>
            if (metaTableKeyWrapper.aliasName != None) {
              SuggestItem(metaTableKeyWrapper.aliasName.get, metaTable, Map("desc" -> "alias"))
            } else {
              SuggestItem(metaTable.key.table, metaTable, Map("desc" -> "table"))
            }
        }.toList
        //如果第一个关键字不是from 或者join关键字处不提示，其他地方都提示
        if (backAndFirstIs(SqlBaseLexer.JOIN) || backAndFirstIs(SqlBaseLexer.FROM)) {
          List()
        } else {
          //解决from后查询的表是`${}` 不写别名，在列中也能提示`${}`中表名问题
          val tables: List[MetaTable] = selectSuggester.context.tempTableProvider.setList(Map())
          var filterSuggester: List[SuggestItem] = List()
          for (a <- 0 until suggestList.size) {
            var i = -1
            var isAlias = -1
            for (b <- 0 until tables.size) {
              if (i == -1 && suggestList(a).extra.getOrElse("desc", "None").equals("table") &&
                tables(b).key.table.equals(suggestList(a).name)) {
                i = 1
              }
            }
            if (i == -1) {
              filterSuggester = filterSuggester :+ suggestList(a)
            }
          }
          filterSuggester
        }


      case None =>
        val tokenPrefix = LexerUtils.tableTokenPrefix(tokens, tokenPos)
        val owner = AutoSuggestContext.context().reqParams.getOrElse("owner", "")
        val extraParam = Map("searchPrefix" -> tokenPrefix, "owner" -> owner)
        selectSuggester.context.metaProvider.list(extraParam).map { item =>
          SuggestItem(item.key.table, item, Map())
        }
    }
  }


  def attributeSuggest(): List[SuggestItem] = {
    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"attributeSuggest:\n")
      logInfo("========attributeSuggest start=======")
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build

    def allOutput = {
      /**
        * 优先推荐别名
        */
      val table = temp.getMatchTokens.head.getText
      val res = table_info.get.filter { item => item._2.key == SpecialTableConst.subQueryAliasTable && (item._1.aliasName.isDefined && item._1.aliasName.equals(table)) }.flatMap { table =>
        if (selectSuggester.context.isInDebugMode) {
          val columns = table._2.columns.map { item => s"${item.name} ${item}" }.mkString("\n")
          logInfo(s"TARGET table: ${table._1} \n columns: \n[${columns}] ")
        }
        table._2.columns.map(column => SuggestItem(column.name, table._2, Map("desc" -> "cloumn")))

      }.toList

      if (res.isEmpty) {
        if (selectSuggester.context.isInDebugMode) {
          val tables = table_info.get.map { case (key, table) =>
            val columns = table.columns.map { item => s"${item.name} ${item}" }.mkString("\n")
            s"${key}:\n ${columns}"
          }.mkString("\n")
          logInfo(s"ALL tables: \n ${tables}")
        }
        // 解决select中查询表后，出现任何符合mlsql词法的字符都可以提示列名问题
        if (temp.isSuccess) {
          res
        } else {
          table_info.get.flatMap { case (_, metaTable) =>
            metaTable.columns.map(column => SuggestItem(column.name, metaTable, Map("desc" -> "cloumn"))).toList
          }.toList
        }
      } else res

    }

    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"Try to match attribute db prefix: Status(${temp.isSuccess})")
    }

    val res = if (temp.isSuccess) {
      val table = temp.getMatchTokens.head.getText
      table_info.get.filter { case (key, value) =>
        (key.aliasName.isDefined && key.aliasName.get == table) || key.metaTableKey.table == table
      }.headOption match {
        case Some(table) =>
          if (selectSuggester.context.isInDebugMode) {
            logInfo(s"table[${table._1}] found, return ${table._2.key} columns.")
          }
          table._2.columns.map(column => SuggestItem(column.name, table._2, Map("desc" -> "cloumn"))).toList
        case None =>
          if (selectSuggester.context.isInDebugMode) {
            logInfo(s"No table found, so return all table[${table_info.get.map { case (_, metaTable) => metaTable.key.toString }}] columns.")
          }
          allOutput
      }
    } else allOutput

    if (selectSuggester.context.isInDebugMode) {
      logInfo("========attributeSuggest end=======")
    }
    res

  }

  def functionSuggest(): List[SuggestItem] = {
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"functionSuggest:\n")
      logInfo("========functionSuggest start=======")
    }

    def allOutput = {
      val udfFuns: List[MetaTable] = selectSuggester.context.tempTableProvider.udfFunList(Map())
      udfFuns.map(item => SuggestItem(item.key.table, item, Map("desc" -> "udf function"))) ++
        MLSQLSQLFunction.funcMetaProvider.list(Map()).map(item => SuggestItem(item.key.table, item, Map("desc" -> "function")))
    }

    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    // 如果匹配上了，说明是字段，那么就不应该提示函数了
    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    val res = if (temp.isSuccess) {
      List()
    } else allOutput

    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"functions: ${allOutput.map(_.name).mkString(",")}")
      logInfo("========functionSuggest end=======")
    }
    res

  }
}
