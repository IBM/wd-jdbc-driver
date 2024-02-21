/**
  * based on
  * https://github.com/antlr/grammars-v4/blob/master/sql/postgresql/PostgreSQLLexer.g4
  */
lexer grammar WDSQLLexer;

options {

}

@header {
}

@members {
}

SCOL
    : ';'
    ;

DOT
    : '.'
    ;

OPEN_PAR
    : '('
    ;

CLOSE_PAR
    : ')'
    ;

COMMA
    : ','
    ;

STAR
    : '*'
    ;

AS_
    : 'AS'
    | 'as'
    ;

FROM_
    : 'FROM'
    | 'from'
    ;

SELECT_
    : 'SELECT'
    | 'select'
    ;


IDENTIFIER
    : '"' (~'"' | '""')* '"'
    | '`' (~'`' | '``')* '`'
    | '[' ~']'* ']'
    | [a-zA-Z_] [a-zA-Z_0-9]*
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'')* '\''
    ;

