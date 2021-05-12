package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.core.datasource.DataSourceRegistry
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
  * Created by Lenovo on 2021/3/22.
  */
class SaveSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {
  override def name: String = "save"

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()
  register(classOf[SaveModelSuggester])
  register(classOf[SaveSrcViewleSuggester])
  register(classOf[SaveAsSuggester])
  register(classOf[SavePathQuoteSuggester])
  register(classOf[SaveWhereSuggester])


  def SPLIT_KEY_WORDS = {
    List(DSLSQLLexer.OVERWRITE, DSLSQLLexer.APPEND, DSLSQLLexer.AS)
  }


  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SAVE) => true
      case _ => false
    }
  }

  private def keywordSuggest(): List[SuggestItem] = {
    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, offsetInToken) =>
        var items = List[SuggestItem]()
        val biOver = matchQISuggester(DSLSQLLexer.BACKQUOTED_IDENTIFIER,pos,DSLSQLLexer.OVERWRITE)
        val biAppnd = matchQISuggester(DSLSQLLexer.BACKQUOTED_IDENTIFIER,pos,DSLSQLLexer.APPEND)
        val iAppend = matchQISuggester(DSLSQLLexer.IDENTIFIER,pos,DSLSQLLexer.APPEND)
        val iOver = matchQISuggester(DSLSQLLexer.IDENTIFIER,pos,DSLSQLLexer.OVERWRITE)

        if (biOver.isSuccess || biAppnd.isSuccess || iAppend.isSuccess || iOver.isSuccess) {
          items = List(SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items

      case _ => List()
    }
  }


  def matchQISuggester(rule: Int, pos:Int, model:Int) :TokenMatcher = {
    TokenMatcher(_tokens, pos).back.
      eat(Food(None, rule),Food(None, model),Food(None, DSLSQLLexer.SAVE)).build
  }

  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subInstances.toMap)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[SaveSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}


class SaveModelSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "model"

  override def isMatch(): Boolean = {
    (tokenPos.pos, tokenPos.currentOrNext) match {
      case (0, TokenPosType.NEXT) => true
      case (1, TokenPosType.CURRENT) => true
      case (_, _) => false
    }

  }

  override def suggest(): List[SuggestItem] = {
    val sources = (Seq("overwrite", "append")).toList
    LexerUtils.filterPrefixIfNeeded(
      sources.map(SuggestItem(_, SpecialTableConst.SAVE_MODEL,
        Map("desc" -> "model"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos

  def context = saveSuggester.context
}

class SaveSrcViewleSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "srcView"

  override def isMatch(): Boolean = {
    //backAndFirstIs(DSLSQLLexer.OVERWRITE,saveSuggester.SPLIT_KEY_WORDS) ||
    //(DSLSQLLexer.APPEND,saveSuggester.SPLIT_KEY_WORDS)
    val temp = TokenMatcher(tokens, tokenPos.pos - 1).back.
      eat(Food(None, DSLSQLLexer.OVERWRITE)).
      eat(Food(None, DSLSQLLexer.APPEND))
      .build
    if (!temp.isSuccess) {
      (tokenPos.pos, tokenPos.currentOrNext) match {
        //如果save append后用户没有输入 按Ctrl + shift
        case (pos, TokenPosType.NEXT) =>
          if (tokens(pos).getType == DSLSQLLexer.OVERWRITE || tokens(pos).getType == DSLSQLLexer.APPEND) true else false

        //如果save append 后用户输入了
        case (2, TokenPosType.CURRENT) =>
          if (tokens(1).getType == DSLSQLLexer.OVERWRITE || tokens(1).getType == DSLSQLLexer.APPEND) true else false
        case _ => false
      }
    } else {
      temp.isSuccess
    }
  }

  override def suggest(): List[SuggestItem] = {
    getTempTable(context)
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos

  def context = saveSuggester.context
}




/*class SaveKeyWordSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "keyWord"

  override def isMatch(): Boolean = {

    //backAndFirstIs(DSLSQLLexer.AS,saveSuggester.SPLIT_KEY_WORDS)
  }

  override def suggest(): List[SuggestItem] = {
    List()
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos

  def context = saveSuggester.context
}*/





class SaveAsSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "as"

  override def isMatch(): Boolean = {
    val temp = TokenMatcher(tokens, tokenPos.pos - 1).back.
      eat(Food(None, DSLSQLLexer.AS)).build
    if (!temp.isSuccess) {
      (tokenPos.pos, tokenPos.currentOrNext) match {
        case (3, TokenPosType.NEXT) => true
        case _ => false
      }
    } else {
      temp.isSuccess
    }
    //backAndFirstIs(DSLSQLLexer.AS,saveSuggester.SPLIT_KEY_WORDS)
  }

  override def suggest(): List[SuggestItem] = {
    val sources = (DataSourceRegistry.allSourceNames.toSet.toSeq ++ Seq(
      "parquet", "csv", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
      "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
    )).toList.filter(f => !f.equals("jsonStr"))
    LexerUtils.filterPrefixIfNeeded(
      sources.map(SuggestItem(_, SpecialTableConst.DATA_SOURCE_TABLE,
        Map("desc" -> "DataSource"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos

  def context = saveSuggester.context
}


class SavePathQuoteSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
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

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos
}


class SaveWhereSuggester(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "where"

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.WHERE) || backAndFirstIs(DSLSQLLexer.OPTIONS)
  }

  override def suggest(): List[SuggestItem] = {
    val sources = Seq("flieNum", "withoutColumns", "withColumns", "conditionExpr").toList
    LexerUtils.filterPrefixIfNeeded(
      sources.map(SuggestItem(_, SpecialTableConst.DATA_SOURCE_TABLE,
        Map("desc" -> "condition"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos

  def context = saveSuggester.context
}