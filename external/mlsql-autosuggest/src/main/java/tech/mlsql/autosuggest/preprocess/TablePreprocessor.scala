package tech.mlsql.autosuggest.preprocess

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.SpecialTableConst.TEMP_TABLE_DB_KEY
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey, StatementTempTableProvider}
import tech.mlsql.autosuggest.statement.{PreProcessStatement, SelectSuggester}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

/**
  * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class TablePreprocessor(context: AutoSuggestContext) extends PreProcessStatement {

  def cleanStr(str: String) = {
    if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''")))
      str.substring(1, str.length - 1)
    else str
  }

  def clearSetStr(setStr: String) = {
    if (setStr.size >= 5 && (setStr.startsWith("`${") || "\"${".equals(setStr.substring(0, 3))) && !"}".equals(setStr.substring(3, 4)))
      setStr.substring(3, setStr.length - 2)
    else setStr
  }

  def backQuote(tble: String) = {
    if (tble.startsWith("`") && tble.endsWith("`") && "${".equals(tble.substring(1, 3)) && tble.endsWith("}`")) {
      tble
    } else {
      if (!tble.startsWith("`") && !tble.endsWith("`"))
        tble
      else ""
    }
  }

  //解析
  def process(statement: List[Token]): Unit = {
    val tempTableProvider = context.tempTableProvider //获取存储表名对象

    statement(0).getText.toLowerCase match {
      case "set" => setTableInfo(statement, tempTableProvider)
      case "register" => registerFun(statement, tempTableProvider)
      case _ => getLoadAndSelectTableInfo(statement, tempTableProvider)
    }
    /*val matcher: TokenMatcher = getLoadAndSelectTableInfo(statement,tempTableProvider)
    if (!matcher.isSuccess) {
      setTableInfo(statement,tempTableProvider)
    }*/
  }


  def registerFun(statement: List[Token], tempTableProvider: StatementTempTableProvider): TokenMatcher = {
    val udfFunStart = TokenMatcher(statement.slice(0, statement.size), 0).asStart(Food(None, DSLSQLLexer.AS), 1).start
    val tempMatcher = TokenMatcher(statement, udfFunStart).back.eat(Food(None, DSLSQLLexer.IDENTIFIER), Food(None, DSLSQLLexer.AS)).build
    if (tempMatcher.isSuccess) {
      val funName: String = statement(udfFunStart).getText
      val defaultFun: MetaTable = SpecialTableConst.udfFunction(funName)
      val table = statement(0).getText.toLowerCase match {
        case "register" =>
          tempTableProvider.udfFunRegister(funName, defaultFun)
      }
    }
    tempMatcher
  }


  def setTableInfo(statement: List[Token], tempTableProvider: StatementTempTableProvider): TokenMatcher = {
    val tempMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.STRING), Food(None, DSLSQLLexer.T__2)).build
    val tempBlockMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.BLOCK_STRING), Food(None, DSLSQLLexer.T__2)).build
    if (tempMatcher.isSuccess || tempBlockMatcher.isSuccess) {
      var tableName = statement(1).getText
      val defaultTable = SpecialTableConst.setTextTempVAR(tableName)
      val sti = statement(0).getText.toLowerCase match {
        case "set" =>
          val tokens: List[Token] = statement.filter(f => f.getType == DSLSQLLexer.WHERE)
          val setHintInfo = if (tokens.size != 0) {
            val whereIndex: Int = statement.indexOf(tokens.head)
            val token: Token = statement(whereIndex + 3)
            val table = cleanStr(token.getText) match {
              case "text" => defaultTable
              case "shell" => SpecialTableConst.setShellTempVAR(tableName)
              case "sql" => SpecialTableConst.setSqlTempVAR(tableName)
              case "defaultParam" => SpecialTableConst.setDefaultParamTempVAR(tableName)
              case _ => MetaTable(MetaTableKey(None, None, null), List(MetaTableColumn(null, null, true, Map())))
            }
            table
          } else {
            defaultTable
          }
          setHintInfo
          tempTableProvider.setRegister(tableName, setHintInfo)
          tempTableProvider.setKVRegister(tableName, statement.map(m => m.getText + " ").mkString(" "))
        case _ =>
      }
      //tempTableProvider.setRegister(tableName,sti)
    }
    if (tempMatcher.isSuccess) tempMatcher else tempBlockMatcher
  }

  /**
    * //提取load和select语句中表名，属性名
    * load 语句和select语句比较特殊
    * Load语句要获取 真实表
    * select 语句要获取最后的select 语句
    *
    * load语句获取真实表的时候会加一个prefix前缀，该值等于load语句里的format
    */
  def getLoadAndSelectTableInfo(statement: List[Token], tempTableProvider: StatementTempTableProvider): TokenMatcher = {
    val tempMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.IDENTIFIER), Food(None, DSLSQLLexer.AS)).build //判断语句中是否存在表名(as tableName)
    val setTempMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER), Food(None, DSLSQLLexer.AS)).build
    if (tempMatcher.isSuccess || setTempMatcher.isSuccess) {
      var tableName = tempMatcher.getMatchTokens.last.getText
      tableName = backQuote(tableName)
      tableName = clearSetStr(tableName)
      val defaultTable = SpecialTableConst.tempTable(tableName) //默认表信息MetaTable
      val table = statement(0).getText.toLowerCase match {
        case "load" =>
          val formatMatcher = TokenMatcher(statement, 1).
            eat(Food(None, DSLSQLLexer.IDENTIFIER),
              Food(None, MLSQLTokenTypeWrapper.DOT),
              Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).build
          if (formatMatcher.isSuccess) {
            val directQuerySql: List[Token] = statement.filter(f => f.getType == DSLSQLLexer.BLOCK_STRING)
            formatMatcher.getMatchTokens.map(_.getText) match {
              case List(format, _, path) =>
                //如果laod加载的是脚本，将脚本名称存入集合
                if (format.toLowerCase.equals("script")) {
                  context.tempTableProvider.scriptRegister(tableName, SpecialTableConst.tempTable(tableName))
                }
                cleanStr(path).split("\\.", 2) match {
                  case Array(db, table) =>
                    //                    if(context.isSchemaInferEnabled){
                    //
                    //                    }
                    if (directQuerySql.size != 0) {
                      context.metaProvider.search(MetaTableKey(Option(format), Option(db), table), Map("sql" -> directQuerySql.head.getText, "table" -> tableName)).getOrElse(defaultTable)
                    } else {
                      context.metaProvider.search(MetaTableKey(Option(format), Option(db), table), Map("table" -> tableName)).getOrElse(defaultTable)
                    }
                  case Array(table) =>
                    if (!table.contains("\\.") || !table.contains("\\/") || !table.contains("\\")) {
                      val setStatement = context.tempTableProvider.setKVSearch(table)
                      context.metaProvider.search(MetaTableKey(Option(format), None, table), Map("set" -> setStatement.getOrElse("None"), "table" -> tableName)).getOrElse(defaultTable)
                    } else {
                      context.metaProvider.search(MetaTableKey(Option(format), None, table), Map("table" -> tableName)).getOrElse(defaultTable)
                    }
                }
            }
          } else {
            defaultTable
          }
        case "select" =>
          //statement.size - 3 是为了移除 最后的as table;语句,为了防止将表名添加到列存储中，， slice语句跟subString方法一样，截前不截后
          val selectSuggester = new SelectSuggester(context, statement.slice(0, statement.size - 3), TokenPos(0, TokenPosType.NEXT, -1))
          val columns = selectSuggester.sqlAST.output(selectSuggester.tokens).map { name =>
            MetaTableColumn(name, null, true, Map())
          }
          MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), tableName), columns)
        case _ => defaultTable
      }
      if (!statement(0).getText.toLowerCase.equals("connect")) {
        tempTableProvider.register(tableName, table)
      }
    }
    if (tempMatcher.isSuccess) tempMatcher else setTempMatcher
  }

}
