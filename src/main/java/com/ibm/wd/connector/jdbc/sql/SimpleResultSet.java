package com.ibm.wd.connector.jdbc.sql;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class SimpleResultSet extends ReadOnlyResultSet {

    protected final SimpleResultSetMetaData metadata;
    private final Statement statement;
    private final List<Map<String, Object>> rows;

    private boolean closed = false;
    private int currentRow = 0;
    private int fetchDirection;

    SimpleResultSet(
            SimpleResultSetMetaData.Builder builder,
            Statement statement,
            List<Map<String, Object>> rows
    ) throws SQLException {
        super();
        this.metadata = builder.build();
        this.statement = statement;
        this.rows = rows;
        this.fetchDirection = statement.getFetchDirection();
    }

    @Override
    public boolean next() throws SQLException {
        if (getFetchDirection() == ResultSet.FETCH_REVERSE) {
            return relative(-1);
        }
        return relative(1);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Map<String, Object> rowObject = rows.get(currentRow - 1);
        return rowObject.get(metadata.getColumnName(columnIndex));
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public int getRow() throws SQLException {
        return currentRow;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentRow == rows.size();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentRow > rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentRow == 1;
    }

    @Override
    public boolean first() throws SQLException {
        return absolute(1);
    }

    @Override
    public boolean last() throws SQLException {
        return absolute(rows.size());
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (1 <= row && row <= rows.size()) {
            currentRow = row;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        int newRow = currentRow + rows;
        if (1 <= newRow && newRow <= this.rows.size()) {
            currentRow = newRow;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        return relative(-1);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return metadata.findColumnByLabel(columnLabel);
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

}
