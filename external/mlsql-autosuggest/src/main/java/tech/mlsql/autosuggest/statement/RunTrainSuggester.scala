package tech.mlsql.autosuggest.statement


import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.utils.PlugGatherInit
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
  * Created by Lenovo on 2021/3/29.
  */
class RunTrainSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {
  override def name: String = "run & train"

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()
  register(classOf[RunTrainSrcTableSuggester])
  register(classOf[RunTrainPlugSuggester])
  register(classOf[RunTrainPathQuoteSuggester])
  register(classOf[RunTrainWhereOrASSuggester])
  register(classOf[RunTrainWhereSuggester])
  register(classOf[RunTrainTargetTableSuggester])

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.RUN) => true
      case Some(DSLSQLLexer.TRAIN) => true
      case _ => false
    }
  }


  private def keywordSuggest: List[SuggestItem] = {
    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, offsetInToken) =>
        var items = List[SuggestItem]()
        val biRun = matchQISuggester(DSLSQLLexer.BACKQUOTED_IDENTIFIER,pos,DSLSQLLexer.RUN)
        val iRun = matchQISuggester(DSLSQLLexer.IDENTIFIER,pos,DSLSQLLexer.RUN)

        val biTrain = matchQISuggester(DSLSQLLexer.BACKQUOTED_IDENTIFIER,pos,DSLSQLLexer.TRAIN)
        val iTrain = matchQISuggester(DSLSQLLexer.IDENTIFIER,pos,DSLSQLLexer.TRAIN)

        if (biRun.isSuccess || iRun.isSuccess || biTrain.isSuccess || iTrain.isSuccess) {
          items = List(SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items

      case _ => List()
    }
  }


  def matchQISuggester(rule: Int, pos:Int, rt:Int) :TokenMatcher = {
    TokenMatcher(_tokens, pos).back.
      eat(Food(None, rule),Food(None, rt)).build
  }


  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subInstances.toMap)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[RunTrainSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}

class RunTrainSrcTableSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "srcTable"

  override def isMatch(): Boolean = {
    (tokenPos.pos, tokenPos.currentOrNext) match {
      case (0, TokenPosType.NEXT) => true
      case (1, TokenPosType.CURRENT) => true
      case (_, _) => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    getTempTable(context, false)
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos

  def context = runTrainSuggester.context
}


class RunTrainPlugSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "plug"

  override def isMatch(): Boolean = {
    (tokenPos.pos, tokenPos.currentOrNext) match {
      case (2, TokenPosType.NEXT) =>
        val temp = TokenMatcher(tokens, tokenPos.pos).back.
          eat(Food(None, DSLSQLLexer.AS)).build
        temp.isSuccess

      case (3, TokenPosType.CURRENT) =>
        val temp = TokenMatcher(tokens, tokenPos.pos - 1).back.
          eat(Food(None, DSLSQLLexer.AS)).build
        temp.isSuccess

      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    val runOrTrain: String = tokens(0).getText.toLowerCase
    if (runOrTrain.equals("run")) {
      val baseList: List[SuggestItem] = mapToSuggestItem(PlugGatherInit.getBaseMap)
      LexerUtils.filterPrefixIfNeeded(baseList, tokens, tokenPos)

    } else if (runOrTrain.equals("train")) {
      val algorList: List[SuggestItem] = mapToSuggestItem(PlugGatherInit.getAlgorithmMap)
      val deepList: List[SuggestItem] = mapToSuggestItem(PlugGatherInit.getDeepLearningMap)
      val featList: List[SuggestItem] = mapToSuggestItem(PlugGatherInit.getFeaturengineerMap)
      LexerUtils.filterPrefixIfNeeded(algorList ++ deepList ++ featList, tokens, tokenPos)
    } else List()

  }

  def mapToSuggestItem(plugInfo : mutable.HashMap[String,String]): List[SuggestItem] = {
    plugInfo.map {
      case (key, value) =>
        SuggestItem(key, SpecialTableConst.PLUG_IN, Map("desc" -> "plug"))
    }.toList
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos

  def context = runTrainSuggester.context
}



class RunTrainPathQuoteSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "pathQuote"

  override def isMatch(): Boolean = {
    val temp = TokenMatcher(tokens, tokenPos.pos).back.
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).
      eat(Food(None, DSLSQLLexer.AS)).build
    temp.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos
}


class RunTrainWhereOrASSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "whereOrAs"

  override def isMatch(): Boolean = {
    val temp = TokenMatcher(tokens, tokenPos.pos).back.
      eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).build
    temp.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("where", SpecialTableConst.KEY_WORD_TABLE, Map()),
      SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map())),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos
}




class RunTrainWhereSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "where"

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.WHERE)
  }

  override def suggest(): List[SuggestItem] = {
    if (tokens.size >= 7) {
      val plugName: String = tokens(3).getText
      getPlugParam(plugName, context)
    } else List()
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos


  def context = runTrainSuggester.context
}


class RunTrainTargetTableSuggester(runTrainSuggester: RunTrainSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "targetTable"

  override def isMatch(): Boolean = {
    //val whereStart = TokenMatcher(tokens.slice(0, tokens.size), 0).asStart(Food(None, DSLSQLLexer.WHERE), 0).start
    val ass: List[Token] = tokens.filter(f => f.getType == DSLSQLLexer.AS)
    if (ass.size >= 2) {
      backAndFirstIs(DSLSQLLexer.AS)
    } else {
      false
    }
  }

  override def suggest(): List[SuggestItem] = {
    getTempTable(context)
  }

  override def tokens: List[Token] = runTrainSuggester._tokens

  override def tokenPos: TokenPos = runTrainSuggester._tokenPos

  def context = runTrainSuggester.context
}