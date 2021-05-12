package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
  * 30/6/2020 WilliamZhu(allwefantasy@gmail.com)
  */
class RegisterSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester
  with SuggesterRegister {
  private val subInstances = new mutable.HashMap[String, StatementSuggester]()
  register(classOf[RegisterPlugSuggester])
  register(classOf[RegisterPathQuoteSuggester])
  register(classOf[RegisterScriptNameSuggester])
  register(classOf[RegisterWhereSuggester])

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[RegisterSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.REGISTER) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    keywordSuggest(_tokenPos,_tokens) ++ defaultSuggest(subInstances.toMap)
  }

  override def name: String = "register"
}


class RegisterPlugSuggester(registerSuggester: RegisterSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "plug"

  override def isMatch(): Boolean = {
    (tokenPos.pos, tokenPos.currentOrNext) match {
      case (0, TokenPosType.NEXT) => true
      case (1, TokenPosType.CURRENT) => true
      case (_, _) => false
    }

  }

  override def suggest(): List[SuggestItem] = {
    val sources = (Seq("ScriptUDF", "RandomForest", "WaterMarkInPlace")).toList
    LexerUtils.filterPrefixIfNeeded(
      sources.map(SuggestItem(_, SpecialTableConst.PLUG_IN,
        Map("desc" -> "plug"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = registerSuggester._tokens

  override def tokenPos: TokenPos = registerSuggester._tokenPos

  def context = registerSuggester.context
}



class RegisterPathQuoteSuggester(registerSuggester: RegisterSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "pathQuote"

  override def isMatch(): Boolean = {
    val temp = TokenMatcher(tokens, tokenPos.pos).back.
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).
      eat(Food(None, DSLSQLLexer.REGISTER)).build
    temp.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table")),
      SuggestItem("`${}`", SpecialTableConst.OTHER_TABLE, Map("desc" -> "reference set var"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = registerSuggester._tokens

  override def tokenPos: TokenPos = registerSuggester._tokenPos
}



class RegisterScriptNameSuggester(registerSuggester: RegisterSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "ScriptName"

  override def isMatch(): Boolean = {
    if (tokens.size >= 2 && tokens(1).getText.equals("ScriptUDF")) {
      val temp = TokenMatcher(tokens, tokenPos.pos - 1).back.
        eat(Food(None, DSLSQLLexer.T__0)).
        eat(Food(None, DSLSQLLexer.IDENTIFIER))
        .build
      temp.isSuccess
    } else {
      false
    }
  }

  override def suggest(): List[SuggestItem] = {
    getScriptTable(context)
  }

  override def tokens: List[Token] = registerSuggester._tokens

  override def tokenPos: TokenPos = registerSuggester._tokenPos

  def context = registerSuggester.context
}


class RegisterWhereSuggester(registerSuggester: RegisterSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "where"

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.WHERE) || backAndFirstIs(DSLSQLLexer.OPTIONS)
  }

  override def suggest(): List[SuggestItem] = {
    if (tokens.size >= 2) {
      val scriptType: String = tokens(1).getText
      getPlugParam(scriptType, context)

    } else List()
  }

  override def tokens: List[Token] = registerSuggester._tokens

  override def tokenPos: TokenPos = registerSuggester._tokenPos

  def context = registerSuggester.context
}