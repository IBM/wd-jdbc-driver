/**
  * based on
  * https://github.com/antlr/grammars-v4/blob/master/sql/postgresql/PostgreSQLParser.g4
  */
parser grammar WDSQLParser;

options {
  tokenVocab = WDSQLLexer;
}

@header {
}

@members {
}

root
    : (sql_stmt_list)* EOF
    ;

sql_stmt_list
    : SCOL* sql_stmt (SCOL+ sql_stmt)* SCOL*
    ;

sql_stmt
    : (
      select_stmt
    )
    ;

select_stmt
    : select_core
    ;

select_core
    : (
        SELECT_ result_column (COMMA result_column)* (
            FROM_ (table_or_subquery)
        )?
    )
    ;

result_column
    : STAR
    | column_expr ( AS_? column_alias)?
    ;

table_or_subquery
    : (
        (schema_name DOT)? table_name //(AS_? table_alias)?
    )
//    | OPEN_PAR (table_or_subquery (COMMA table_or_subquery)* ) CLOSE_PAR
//    | OPEN_PAR select_stmt CLOSE_PAR (AS_? table_alias)?
    ;

schema_name
    : any_name
    ;

table_name
    : any_name
    ;

table_alias
    : any_name
    ;

column_expr
    : ((schema_name DOT)? table_name DOT)? column_name
    ;

column_name
    : any_name
    ;

column_alias
    : IDENTIFIER
    | STRING_LITERAL
    ;

any_name
    : IDENTIFIER
    | keyword
    | STRING_LITERAL
    | OPEN_PAR any_name CLOSE_PAR
    ;

keyword
    : AS_
    | FROM_
    | SELECT_
    ;




