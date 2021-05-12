package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{SpecialTableConst, TokenPos, TokenPosType}
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.meta.MetaTable
import tech.mlsql.common.utils.log.Logging

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementSuggester extends Logging{
  def name: String

  def isMatch(): Boolean

  def suggest(): List[SuggestItem]

  def defaultSuggest(subInstances: Map[String, StatementSuggester]): List[SuggestItem] = {
    var instance: StatementSuggester = null
    subInstances.foreach { _instance =>
      if (instance == null && _instance._2.isMatch()) {
        instance = _instance._2
      }
    }
    if (instance == null) List()
    else instance.suggest()
  }

  def keywordSuggest(_tokenPos:TokenPos,_tokens:List[Token]): List[SuggestItem] = {
    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, offsetInToken) =>
        var items = List[SuggestItem]()
        val temp = TokenMatcher(_tokens, pos).back.
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
          build
        if (temp.isSuccess) {
          items = List(SuggestItem("where", SpecialTableConst.KEY_WORD_TABLE, Map()), SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items

      case _ => List()
    }
  }
}

case class SuggestItem(name: String, metaTable: MetaTable, extra: Map[String, String])
