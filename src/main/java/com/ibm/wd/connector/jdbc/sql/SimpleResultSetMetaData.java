package com.ibm.wd.connector.jdbc.sql;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

public class SimpleResultSetMetaData implements ResultSetMetaData {

    private final String tableName;
    private final List<String> columnLabels;
    private final List<String> columnNames;
    private final List<JDBCType> columnTypes;

    public static class Builder {
        private String tableName;
        private List<String> columnLabels = new ArrayList<>();
        private List<String> columnNames = new ArrayList<>();
        private List<JDBCType> columnTypes = new ArrayList<>();

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder addColumn(String label, String name, JDBCType columnType) {
            columnLabels.add(label);
            columnNames.add(name);
            columnTypes.add(columnType);
            return this;
        }

        public SimpleResultSetMetaData build() {
            return new SimpleResultSetMetaData(
                    tableName,
                    columnLabels,
                    columnNames,
                    columnTypes
            );
        }
    }

    private SimpleResultSetMetaData(
            String tableName,
            List<String> columnLabels,
            List<String> columnNames,
            List<JDBCType> columnTypes
    ) {
        this.tableName = tableName;
        this.columnLabels = columnLabels;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public int findColumnByLabel(String columnLabel) {
        return columnLabels.indexOf(columnLabel) + 1;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnLabels.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 65536;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnLabels.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columnTypes.get(column - 1).getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnTypes.get(column - 1).getName();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }
    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }
}
