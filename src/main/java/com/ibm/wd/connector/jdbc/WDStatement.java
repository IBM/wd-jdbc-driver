package com.ibm.wd.connector.jdbc;

import com.ibm.wd.connector.jdbc.model.*;
import com.ibm.wd.connector.jdbc.sql.SelectStatement;
import com.ibm.wd.connector.jdbc.sql.WDQueryResultSet;
import com.ibm.wd.connector.jdbc.sql.WDResultSetMetaData;
import com.ibm.wd.connector.jdbc.sql.WDSQLParser;
import com.ibm.wd.connector.jdbc.support.WDInfoStore;

import java.sql.*;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_DEFAULT_FETCH_SIZE;

public class WDStatement implements Statement {

  private final WDConnection connection;
  private final int resultSetType;
  private final int resultSetConcurrency;
  private final int resultSetHoldability;

  private int fetchSize;

  private boolean closed = false;
  private int fetchDirection = ResultSet.FETCH_FORWARD;

  private ResultSet resultSet;


  public WDStatement(WDConnection connection) {
    this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  public WDStatement(WDConnection connection, int resultSetType, int resultSetConcurrency) {
    this(connection, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  public WDStatement(WDConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    this.connection = connection;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    this.fetchSize = Integer.parseInt(WD_DEFAULT_FETCH_SIZE.get(connection.getProperties()));
  }

  public Properties getProperties() { return connection.getProperties(); }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {

    SelectStatement stmt = new WDSQLParser().parse(sql);

    final String schemaName = stmt.getSchemaName();
    final String tableName = stmt.getTableName();

    if (schemaName == null || tableName == null) {
      throw new SQLException("Unsupported SQL statement. sql=" + sql);
    }

    WDInfoStore infoStore = new WDInfoStore(connection.getDiscoveryClient(), connection.getProperties());

    final Optional<WDSchemaInfo> schemaInfo = infoStore.fetchSchemaInfoList().stream()
            .filter(schema -> schema.getSchemaName().equals(schemaName))
            .findFirst();

    if (!schemaInfo.isPresent()) {
      throw new SQLException("Schema " + schemaName + " not found");
    }

    final Optional<WDTableInfo> tableInfo = infoStore.fetchTableInfoList(schemaInfo.get().getProjectId()).stream()
            .filter(table -> table.getTableName().equals(tableName))
            .findFirst();

    if (!tableInfo.isPresent()) {
      throw new SQLException("Table " + tableName + " not found");
    }

    WDResultSetMetaData.Builder builder = new WDResultSetMetaData.Builder()
            .tableName(tableName);

    if (stmt.isUseAllColumns()) {
      for (WDColumnInfo columnInfo : tableInfo.get().getColumns()) {
        builder = builder.addColumn(columnInfo);
      }
    } else {
      List<String> columnNames = stmt.getColumnNames();
      List<String> columnAliases = stmt.getColumnAliases();
      for (int columnIdx = 0; columnIdx < columnNames.size(); columnIdx++) {
        String columnName = columnNames.get(columnIdx);
        String columnAlias = columnAliases.get(columnIdx);
        Optional<WDColumnInfo> columnInfo = tableInfo.get().findColumnByName(columnName);

        if (columnInfo.isPresent()) {
          WDColumnInfo column = columnInfo.get();
          builder = builder.addColumn(
                  new WDColumnInfo.Builder()
                          .columnName(columnName)
                          .columnLabel(columnAlias)
                          .fieldPath(column.getFieldPath())
                          .fieldType(column.getFieldType())
                          .isPrimary(column.getIsPrimary())
                          .build()
          );
        }
      }
    }

    resultSet = new WDQueryResultSet(
            this,
            schemaInfo.get(),
            tableInfo.get(),
            builder,
            connection.getDiscoveryClient()
    );

    return resultSet;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public void close() throws SQLException {
    closed = true;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // do nothing
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    resultSet = executeQuery(sql);
    return true;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public void setFetchSize(int rows) {
    this.fetchSize = rows;
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return fetchDirection;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    this.fetchDirection = direction;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return resultSetType;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return resultSetConcurrency;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return resultSetHoldability;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  /**
   * Not supported features
   */

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0; // no limit
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // do nothing
  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0; // no limit
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    // do nothing
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // do nothing
  }

  @Override
  public void cancel() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // do nothing
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // do nothing
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // do nothing
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }
}
