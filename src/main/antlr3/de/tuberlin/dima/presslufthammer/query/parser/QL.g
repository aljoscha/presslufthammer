/**
 * ANTLR grammar for our simplistic query language, the rules directly
 * create the query using the classes in query.*. 
 *
 * @autor Aljoscha Krettek
 */
grammar QL;

options {
    backtrack=true;
    memoize=true;
}

@header {
  package de.tuberlin.dima.presslufthammer.query.parser;
  import de.tuberlin.dima.presslufthammer.query.*;
  import com.google.common.collect.Lists;
}

@members {
    private List<String> errors = Lists.newLinkedList();
    public void displayRecognitionError(String[] tokenNames,
                                        RecognitionException e) {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        errors.add(hdr + " " + msg);
    }
    public List<String> getErrors() {
        return errors;
    }
}


@lexer::header {
  package de.tuberlin.dima.presslufthammer.query.parser;
}

// Parser section *************************************************************
           
query returns [Query result]
@init {
    List<SelectClause> selectClauses = Lists.newLinkedList();
    List<WhereClause> whereClauses = Lists.newLinkedList();
    List<String> groupByColumns = Lists.newLinkedList();
}
    : 'SELECT'
      sc1=selectClause { selectClauses.add($sc1.result); }
      (',' scn=selectClause { selectClauses.add($scn.result); })*
      'FROM'
      tableName=IDENTIFIER
      (':' partition=INTLITERAL)?
      
      (
      'WHERE'
      wc1=whereClause { whereClauses.add($wc1.result); }
      (',' wcn=whereClause { whereClauses.add($wcn.result); })*
      )?
      
      (
      'GROUP BY'
      gb1=IDENTIFIER { groupByColumns.add($gb1.getText()); }
      (',' gbn=IDENTIFIER { groupByColumns.add($gbn.getText()); })*
      )?
      {
        int part = -1;
        if ($partition != null) {
          part = Integer.parseInt($partition.getText());
        }
        $result = new Query($tableName.getText(), part,
                            selectClauses, whereClauses, groupByColumns);
      }
    ;
    
selectClause returns [SelectClause result]
    : '*'
      {
        $result = new SelectClause("*", null);
      }
    | column=IDENTIFIER
      ( 'AS' rename=IDENTIFIER )?
      {
        String renameAs = null;
        if ($rename != null) {
          renameAs = $rename.getText();
        }
        $result = new SelectClause($column.getText(), renameAs);
      }
    ;
    
whereClause returns [WhereClause result]
    : column=IDENTIFIER '==' literal=STRINGLITERAL
      {
        $result = new WhereClause($column.getText(),
                                  WhereClause.Op.EQ,
                                  $literal.getText());
      }
    | column=IDENTIFIER '!=' literal=STRINGLITERAL
      {
        $result = new WhereClause($column.getText(),
                                  WhereClause.Op.NEQ,
                                  $literal.getText());
      }
    | column=IDENTIFIER '==' literal=INTLITERAL
      {
        $result = new WhereClause($column.getText(),
                                  WhereClause.Op.EQ,
                                  $literal.getText());
      }
    | column=IDENTIFIER '!=' literal=INTLITERAL
      {
        $result = new WhereClause($column.getText(),
                                  WhereClause.Op.NEQ,
                                  $literal.getText());
      }
    ;
// Lexer section **************************************************************

LONGLITERAL
    :   IntegerNumber LongSuffix
    ;

    
INTLITERAL
    :   IntegerNumber 
    | '-' IntegerNumber
    ;
    
fragment
IntegerNumber
    :   '0' 
    |   '1'..'9' ('0'..'9')*    
    |   '0' ('0'..'7')+         
    |   HexPrefix HexDigit+        
    ;

fragment
HexPrefix
    :   '0x' | '0X'
    ;
        
fragment
HexDigit
    :   ('0'..'9'|'a'..'f'|'A'..'F')
    ;

fragment
LongSuffix
    :   'l' | 'L'
    ;


fragment
NonIntegerNumber
    :   ('0' .. '9')+ '.' ('0' .. '9')* Exponent?  
    |   '.' ( '0' .. '9' )+ Exponent?  
    |   ('0' .. '9')+ Exponent  
    |   ('0' .. '9')+ 
    |   
        HexPrefix (HexDigit )* 
        (    () 
        |    ('.' (HexDigit )* ) 
        ) 
        ( 'p' | 'P' ) 
        ( '+' | '-' )? 
        ( '0' .. '9' )+
        ;
        
fragment 
Exponent    
    :   ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ 
    ;
    
fragment 
FloatSuffix
    :   'f' | 'F' 
    ;     

fragment
DoubleSuffix
    :   'd' | 'D'
    ;
        
FLOATLITERAL
    :   NonIntegerNumber FloatSuffix
    ;
    
DOUBLELITERAL
    :   NonIntegerNumber DoubleSuffix?
    ;

CHARLITERAL
    :   '\'' 
        (   EscapeSequence 
        |   ~( '\'' | '\\' | '\r' | '\n' )
        ) 
        '\''
    ; 

STRINGLITERAL
    :   '"' 
        (   EscapeSequence
        |   ~( '\\' | '"' | '\r' | '\n' )        
        )* 
        '"' 
    ;

fragment
EscapeSequence 
    :   '\\' (
                 'b' 
             |   't' 
             |   'n' 
             |   'f' 
             |   'r' 
             |   '\"' 
             |   '\'' 
             |   '\\' 
             |       
                 ('0'..'3') ('0'..'7') ('0'..'7')
             |       
                 ('0'..'7') ('0'..'7') 
             |       
                 ('0'..'7')
             )          
;     

WS  
    :   (
             ' '
        |    '\r'
        |    '\t'
        |    '\u000C'
        |    '\n'
        ) 
            {
                skip();
            }          
    ;
    
IDENTIFIER
    :   IdentifierStart IdentifierPart*
    ;

fragment
IdentifierStart
    : 'A'..'Z'
    | 'a'..'z'
    ;                
                       
fragment 
IdentifierPart
    : 'A'..'Z'
    | 'a'..'z'
    | '.'
    ;

