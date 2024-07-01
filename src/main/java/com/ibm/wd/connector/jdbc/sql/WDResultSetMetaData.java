package com.ibm.wd.connector.jdbc.sql;

import com.ibm.wd.connector.jdbc.model.WDColumnInfo;
import com.ibm.wd.connector.jdbc.model.WDFieldType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WDResultSetMetaData implements ResultSetMetaData {

    private final String tableName;
    private final List<WDColumnInfo> columns;

    public static class Builder {
        private String tableName;
        private List<WDColumnInfo> columns = new ArrayList<>();

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder addColumn(WDColumnInfo column) {
            columns.add(column);
            return this;
        }

        public WDResultSetMetaData build() {
            return new WDResultSetMetaData(
                    tableName,
                    columns
            );
        }
    }

    private WDResultSetMetaData(
            String tableName,
            List<WDColumnInfo> columns
    ) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public int findColumnByLabel(String columnLabel) {
        for (int i = 0; i < columns.size(); i++) {
            if (Objects.equals(columns.get(i).getColumnLabel(), columnLabel)) {
                return i + 1;
            }
        }
        return 0;
    }

    private WDColumnInfo getColumn(int column) {
        return columns.get(column - 1);
    }

    public WDFieldType getWDFieldType(int columnIdx) {
        return columns.get(columnIdx - 1).getFieldType();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
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
        return getColumn(column).getColumnLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getColumnName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getFieldType().getJdbcType().getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getFieldType().getJdbcType().getName();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumn(column).getFieldType().getClazz().getName();
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
        return getColumn(column).getFieldType().getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }
    @Override
    public boolean isSigned(int column) throws SQLException {
        return !getColumn(column).getFieldType().isUnsigned();
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
