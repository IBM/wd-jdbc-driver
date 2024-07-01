package com.ibm.wd.connector.jdbc.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SelectStatement {
    private String schemaName;
    private String tableName;
    private boolean useAllColumns;
    private List<String> columnNames;
    private List<String> columnAliases;

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isUseAllColumns() {
        return useAllColumns;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnAliases() {
        return columnAliases;
    }

    public static class Builder {
        private String schemaName;
        private String tableName;
        private boolean useAllColumns;
        private List<String> columnNames = new ArrayList<>();
        private List<String> columnAliases = new ArrayList<>();

        private String normalizeStr(String str) {
            if (str != null && str.length() > 0 && str.startsWith("\"") && str.endsWith("\"")) {
                return str.substring(1, str.length() - 1);
            } else {
                return str;
            }
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = normalizeStr(schemaName);
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = normalizeStr(tableName);
            return this;
        }

        public Builder useAllColumns(boolean useAllColumns) {
            this.useAllColumns = useAllColumns;
            return this;
        }

        public Builder addColumn(String columnName, String columnAlias) {
            columnNames.add(normalizeStr(columnName));
            columnAliases.add(normalizeStr(columnAlias != null ? columnAlias : columnName));
            return this;
        }

        public SelectStatement build() {
            SelectStatement stmt = new SelectStatement();
            stmt.schemaName = schemaName;
            stmt.tableName = tableName;
            stmt.useAllColumns = useAllColumns;
            if (!useAllColumns) {
                stmt.columnNames = Collections.unmodifiableList(columnNames);
                stmt.columnAliases = Collections.unmodifiableList(columnAliases);
            }
            return stmt;
        }
    }
}
