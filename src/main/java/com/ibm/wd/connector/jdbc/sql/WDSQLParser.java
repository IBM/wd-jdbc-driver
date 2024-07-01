package com.ibm.wd.connector.jdbc.sql;

import com.ibm.wd.connector.parser.WDSQLLexer;
import com.ibm.wd.connector.parser.WDSQLParserBaseListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.sql.SQLException;

public class WDSQLParser {

    public SelectStatement parse(String sql) {
        WDSQLLexer lexer = new WDSQLLexer(CharStreams.fromString(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        com.ibm.wd.connector.parser.WDSQLParser parser = new com.ibm.wd.connector.parser.WDSQLParser(tokens);
        ParseTreeWalker walker = ParseTreeWalker.DEFAULT;
        Listener listener = new Listener();
        walker.walk(listener, parser.root());

        return listener.getStmt();
    }

    private static class Listener extends WDSQLParserBaseListener {

        private SelectStatement stmt;

        public SelectStatement getStmt() {
            return stmt;
        }

        @Override
        public void enterSelect_core(com.ibm.wd.connector.parser.WDSQLParser.Select_coreContext ctx) {
            SelectStatement.Builder builder = new SelectStatement.Builder()
                    .schemaName(ctx.table_or_subquery().schema_name().getText())
                    .tableName(ctx.table_or_subquery().table_name().getText());
            for (com.ibm.wd.connector.parser.WDSQLParser.Result_columnContext resultColumn : ctx.result_column()) {
                if (resultColumn.STAR() != null) {
                    builder = builder.useAllColumns(true);
                    break;
                }

                com.ibm.wd.connector.parser.WDSQLParser.Column_exprContext columnExpr = resultColumn.column_expr();
                com.ibm.wd.connector.parser.WDSQLParser.Column_aliasContext columnAlias = resultColumn.column_alias();

                builder = builder.addColumn(
                        columnExpr.column_name().getText(),
                        columnAlias == null ? null : columnAlias.getText()
                );
            }

            stmt = builder.build();
            super.enterSelect_core(ctx);
        }
    }
}
