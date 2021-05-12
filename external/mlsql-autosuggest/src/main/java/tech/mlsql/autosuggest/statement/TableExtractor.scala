package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.autosuggest.SpecialTableConst.TEMP_TABLE_DB_KEY
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.autosuggest.meta.MetaTableKey

import scala.collection.mutable.ArrayBuffer

/**
  * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class TableExtractor(autoSuggestContext: AutoSuggestContext, ast: SingleStatementAST, tokens: List[Token]) extends MatchAndExtractor[MetaTableKeyWrapper] {

  def setMatcher(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build

    temp.isSuccess match {
      case true => temp
      case false =>
        val setTemp = TokenMatcher(tokens, start).
          eat(Food(None, SqlBaseLexer.BACKQUOTED_IDENTIFIER), Food(None, SqlBaseLexer.IDENTIFIER)).
          eat(Food(None, SqlBaseLexer.AS)).optional.
          eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
          build
        val setTemp1 = setTemp.isSuccess match {
          case true => setTemp
          case false =>
            TokenMatcher(tokens, start).
              eat(Food(None, SqlBaseLexer.BACKQUOTED_IDENTIFIER), Food(None, SqlBaseLexer.IDENTIFIER)).
              eat(Food(None, SqlBaseLexer.AS)).optional.
              eat(Food(None, SqlBaseLexer.BACKQUOTED_IDENTIFIER)).optional.
              build
        }
        setTemp1
    }
  }

  override def matcher(start: Int): TokenMatcher = {
    //查询语句用户输入临时表后也能在列中提示
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, TokenTypeWrapper.DOT)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build

    //修改地方二：查询语句用户输入临时表和别名后也能在列中提示
    if (temp.isSuccess) {
      temp
    } else {
      val temp = TokenMatcher(tokens, start).
        eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, TokenTypeWrapper.DOT)).optional.
        eat(Food(None, SqlBaseLexer.BACKQUOTED_IDENTIFIER)).
        eat(Food(None, SqlBaseLexer.AS)).optional.
        eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
        build
      if (temp.isSuccess) {
        temp
      } else setMatcher(start)
    }
  }

  override def extractor(start: Int, end: Int): List[MetaTableKeyWrapper] = {
    val dbTableTokens = tokens.slice(start, end)
    val dbTable = dbTableTokens.length match {
      case 2 =>
        val List(tableToken, aliasToken) = dbTableTokens
        if (aliasToken.getText.toLowerCase() == "as") {
          MetaTableKeyWrapper(MetaTableKey(None, None, clearSetStr(tableToken.getText)), None)
        } else {
          //MetaTableKeyWrapper(MetaTableKey(None, None, tableToken.getText), None)
          //查询语句中from后是两个参数，不支持别名提示问题
          MetaTableKeyWrapper(MetaTableKey(None, None, clearSetStr(tableToken.getText)), Option(aliasToken.getText))
        }

      case 3 =>
        val token: List[Token] = dbTableTokens.filter(f => f.getType == SqlBaseLexer.AS)
        if (token.size != 0) {
          val asIndex: Int = dbTableTokens.indexOf(token.head)
          asIndex match {
            case 2 =>
              //如果as在from后的第三个位置，则判定为表名后为别名
              val List(tableToken, aliasToken, _) = dbTableTokens
              MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(tableToken.getText)), Option(aliasToken.getText))

            case 1 =>
              //from后有五个参数的话，如果as在第二个位置，有两种情况，第一种是表名，第二种是别名，
              val List(dbToken, _, tableToken) = dbTableTokens
              //从end开始截取，如果后面两位中还包含as,则认定第一个as后是别名
              val fiveParamAs: List[Token] = tokens.slice(end, end + 2).filter(f => f.getType == SqlBaseLexer.AS)
              if (fiveParamAs.size != 0) {
                MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), Option(tableToken.getText))
              } else {
                //如果只有一个as,且没有用分号结尾,则判定as后为表名,from后只有三个参数,但是如果end后一位是逗号，则认为是别名
                val comma: List[Token] = tokens.slice(end, end + 1).filter(f => f.getType == SqlBaseLexer.T__2)
                val isJoin: List[Token] = tokens.slice(end, end + 4).filter(f => f.getType == SqlBaseLexer.JOIN)
                val isOneJoin: List[Token] = tokens.slice(end, end + 3).filter(f => f.getType == SqlBaseLexer.JOIN)
                val isOn: List[Token] = tokens.slice(end, end + 3).filter(f => f.getType == SqlBaseLexer.ON)
                val isWhere: List[Token] = tokens.slice(end, end + 1).filter(f => f.getType == SqlBaseLexer.WHERE)
                if (comma.size != 0 || isJoin.size != 0 || isOn.size != 0 || isWhere.size != 0 || isOneJoin.size != 0) {
                  MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), Option(tableToken.getText))
                } else {
                  MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), None)
                }
                /*if (isWhere.size == 0 || isOneJoin.size == 0) {
                  MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), Option(tableToken.getText))
                } else {
                  MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), None)
                }*/
              }
          }
        } else {
          val List(dbToken, _, tableToken) = dbTableTokens
          MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), tableToken.getText), None) //问题原因
          //MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), None)
        }
      case 4 =>
        val tokens: List[Token] = dbTableTokens.filter(f => f.getType == SqlBaseLexer.AS)
        if (tokens.size != 0) {
          val asIndex: Int = dbTableTokens.indexOf(tokens.head)
          asIndex match {
            //如果from后有四个参数，as在第三个位置，前两个参数，第一个为表名，第二个为别名
            case 2 =>
              val List(dbToken, aliasToken, _, tableToken) = dbTableTokens
              MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), Option(aliasToken.getText))
          }
        } else {
          val List(dbToken, _, tableToken, aliasToken) = dbTableTokens
          MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), clearSetStr(tableToken.getText)), Option(aliasToken.getText))
        }
      case 5 =>
        val List(dbToken, _, tableToken, _, aliasToken) = dbTableTokens
        MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), clearSetStr(tableToken.getText)), Option(aliasToken.getText))
      //MetaTableKeyWrapper(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), clearSetStr(dbToken.getText)), Option(tableToken.getText))
      case _ => MetaTableKeyWrapper(MetaTableKey(None, None, clearSetStr(dbTableTokens.head.getText)), None)
    }

    List(dbTable)
  }

  override def iterate(start: Int, end: Int, limit: Int = 100): List[MetaTableKeyWrapper] = {
    val tables = ArrayBuffer[MetaTableKeyWrapper]()
    var matchRes = matcher(start)
    var whileLimit = limit
    while (matchRes.isSuccess && whileLimit > 0) {
      tables ++= extractor(matchRes.start, matchRes.get)
      whileLimit -= 1
      val temp = TokenMatcher(tokens, matchRes.get).eat(Food(None, SqlBaseLexer.T__2)).build
      if (temp.isSuccess) {
        matchRes = matcher(temp.get)
      } else whileLimit = 0
    }

    tables.toList
  }

  def clearSetStr(setStr: String) = {
    if (setStr.size > 5 && ("`${".equals(setStr.substring(0, 3)) || "\"${".equals(setStr.substring(0, 3))))
      setStr.substring(3, setStr.length - 2)
    else setStr
  }
}
