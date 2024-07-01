package com.ibm.wd.connector.jdbc.sql;

import com.ibm.wd.connector.jdbc.WDStatement;
import com.ibm.wd.connector.jdbc.model.WDFieldType;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

abstract class WDReadOnlyResultSetBase extends ReadOnlyResultSet {

  protected final WDResultSetMetaData metadata;
  private final WDStatement statement;

  private boolean closed = false;
  private int currentRow = 0;

  WDReadOnlyResultSetBase(WDResultSetMetaData.Builder builder) {
    this(builder, null);
  }

  WDReadOnlyResultSetBase(WDResultSetMetaData.Builder builder, WDStatement statement) {
    super();
    this.metadata = builder.build();
    this.statement = statement;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    for (int columnIdx = 1; columnIdx <= getMetaData().getColumnCount(); columnIdx++) {
      if (getMetaData()
              .getColumnLabel(columnIdx)
              .equals(columnLabel)) {
        return columnIdx;
      }
    }
    return  0;
  }

  abstract boolean doNext() throws SQLException;

  @Override
  public boolean next() throws SQLException {
    boolean hasNext = doNext();
    if (hasNext) {
      ++currentRow;
    }
    return hasNext;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
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
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    return getStatement().getFetchSize();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    getStatement().setFetchSize(rows);
  }

  private void validateColumnType(int columnIndex, WDFieldType... expected) throws SQLException {
    WDFieldType fieldType = metadata.getWDFieldType(columnIndex);
    if (Arrays.stream(expected).noneMatch(exp -> fieldType == exp)) {
      throw new SQLException("invalid type: " + fieldType.getWdType() + " in " + columnIndex + "th column: " + metadata.getColumnName(columnIndex));
    }
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    validateColumnType(columnIndex, WDFieldType.BOOLEAN);
    return super.getBoolean(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    validateColumnType(columnIndex, WDFieldType.LONG, WDFieldType.DOUBLE);
    return super.getLong(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    validateColumnType(columnIndex, WDFieldType.LONG, WDFieldType.DOUBLE);
    return super.getDouble(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    validateColumnType(columnIndex, WDFieldType.LONG, WDFieldType.DOUBLE);
    return super.getBigDecimal(columnIndex);
  }

  @Override
  protected ZonedDateTime getZonedDateTime(int columnIndex, Calendar cal) throws SQLException {
    validateColumnType(columnIndex, WDFieldType.DATE);
    return super.getZonedDateTime(columnIndex, cal);
  }
}
