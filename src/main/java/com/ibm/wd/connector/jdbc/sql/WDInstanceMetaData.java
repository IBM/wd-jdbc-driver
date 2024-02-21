package com.ibm.wd.connector.jdbc.sql;

import com.ibm.wd.connector.jdbc.WDConnection;
import com.ibm.wd.connector.jdbc.WDDriver;
import com.ibm.wd.connector.jdbc.model.WDColumnInfo;
import com.ibm.wd.connector.jdbc.model.WDFieldType;
import com.ibm.wd.connector.jdbc.model.WDSchemaInfo;
import com.ibm.wd.connector.jdbc.model.WDTableInfo;
import com.ibm.wd.connector.jdbc.support.WDInfoStore;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WDInstanceMetaData implements DatabaseMetaData {

    private final WDConnection connection;
    private final WDInfoStore infoStore;

    public WDInstanceMetaData(WDConnection connection) throws SQLException {
        this.connection = connection;
        this.infoStore = new WDInfoStore(connection.getDiscoveryClient(), connection.getProperties());
    }

    public WDInfoStore getWDInfoStore() {
        return this.infoStore;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return this.connection.getDiscoveryClient().getServiceUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return WDDriver.class.getPackage().getName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return WDDriver.class.getPackage().getImplementationVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return WDDriver.class.getPackage().getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return WDDriver.class.getPackage().getImplementationVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return WDDriver.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return WDDriver.getDriverMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return getDriverMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return getDriverMinorVersion();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return connection.getDiscoveryInstanceMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getDiscoveryInstanceMinorVersion();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {

        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("catalogs")
                .addColumn("TABLE_CAT", "TABLE_CAT", JDBCType.VARCHAR);

        List<Map<String, Object>> rows = Stream.of(this.connection.getCatalog())
                .map((catalog) -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put("TABLE_CAT", catalog);
                    return data;
                })
                .collect(Collectors.toList());

        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("table_types")
                .addColumn("TABLE_TYPE", "TABLE_TYPE", JDBCType.VARCHAR);

        List<Map<String, Object>> rows = Stream.of("TABLE")
                .map((type) -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put("TABLE_TYPE", type);
                    return data;
                })
                .collect(Collectors.toList());

        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    private Stream<WDSchemaInfo> streamOfSchema(String schemaPattern) throws SQLException {
        Stream<WDSchemaInfo> schemaStream = infoStore.fetchSchemaInfoList().stream();
        if (schemaPattern != null && !schemaPattern.equals("%")) {
            schemaStream = schemaStream.filter((schema) -> schemaPattern.equals(schema.getSchemaName()));
        }
        return schemaStream;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    /**
     * @param catalog Ignore
     * @param schemaPattern Exact Match Only. If null, all match
     *
     */
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("schemas")
                .addColumn("TABLE_SCHEM", "TABLE_SCHEM", JDBCType.VARCHAR)
                .addColumn("TABLE_CATALOG", "TABLE_CATALOG", JDBCType.VARCHAR);

        final String catalogName = this.connection.getCatalog();

        List<Map<String, Object>> rows = streamOfSchema(schemaPattern)
                .map((schema) -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put("TABLE_SCHEM", schema.getSchemaName());
                    data.put("TABLE_CATALOG", catalogName);
                    return data;
                })
                .collect(Collectors.toList());

        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }


    private Stream<Pair<WDSchemaInfo,WDTableInfo>> streamOfTable(WDSchemaInfo schema, String tableNamePattern) {
        try {
            Stream<WDTableInfo> tableInfoStream = infoStore.fetchTableInfoList(schema.getProjectId()).stream();
            if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                tableInfoStream = tableInfoStream.filter((table) -> tableNamePattern.equals(table.getTableName()));
            }
            return tableInfoStream.map((table) -> new ImmutablePair<>(schema, table));
        } catch (SQLException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    /**
     * @param catalog Ignore
     * @param schemaPattern Exact Match Only. If null, all match
     * @param tableNamePattern Exact Match Only. If null, all match
     * @param types Ignore
     * @return
     * @throws SQLException
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("tables")
                .addColumn("TABLE_CAT", "TABLE_CAT", JDBCType.VARCHAR)
                .addColumn("TABLE_SCHEM", "TABLE_SCHEM", JDBCType.VARCHAR)
                .addColumn("TABLE_NAME", "TABLE_NAME", JDBCType.VARCHAR)
                .addColumn("TABLE_TYPE", "TABLE_TYPE", JDBCType.VARCHAR)
                .addColumn("REMARKS", "REMARKS", JDBCType.VARCHAR)
                .addColumn("TYPE_CAT", "TYPE_CAT", JDBCType.VARCHAR)
                .addColumn("TYPE_SCHEM", "TYPE_SCHEM", JDBCType.VARCHAR)
                .addColumn("TYPE_NAME", "TYPE_NAME", JDBCType.VARCHAR)
                .addColumn("SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COL_NAME", JDBCType.VARCHAR)
                .addColumn("REF_GENERATION", "REF_GENERATION", JDBCType.VARCHAR);

        final String catalogName = this.connection.getCatalog();

        final List<Map<String, Object>> rows = streamOfSchema(schemaPattern)
                .flatMap((schema) -> streamOfTable(schema, tableNamePattern))
                .map((entry) -> {
                    WDSchemaInfo schema = entry.getLeft();
                    WDTableInfo table = entry.getRight();
                    Map<String, Object> data = new HashMap<>();
                    data.put("TABLE_CAT", catalogName);
                    data.put("TABLE_SCHEM", schema.getSchemaName());
                    data.put("TABLE_NAME", table.getTableName());
                    data.put("TABLE_TYPE", "TABLE");
                    data.put("REMARKS", table.getDescription());
                    data.put("TYPE_CAT", catalogName);
                    data.put("TYPE_SCHEM", schema.getSchemaName());
                    // data.put("TYPE_NAME", null);
                    // data.put("SELF_REFERENCING_COL_NAME", null);
                    // data.put("REF_GENERATION", null);
                    return data;
                })
                .collect(Collectors.toList());

        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        //TODO
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("tables")
                .addColumn("TYPE_NAME", "TYPE_NAME", JDBCType.VARCHAR)
                .addColumn("DATA_TYPE", "DATA_TYPE", JDBCType.INTEGER)
                .addColumn("PRECISION", "PRECISION", JDBCType.INTEGER)
                .addColumn("LITERAL_PREFIX", "LITERAL_PREFIX", JDBCType.VARCHAR)
                .addColumn("LITERAL_SUFFIX", "LITERAL_SUFFIX", JDBCType.VARCHAR)
                .addColumn("CREATE_PARAMS", "CREATE_PARAMS", JDBCType.VARCHAR)
                .addColumn("COLUMN_SIZE", "COLUMN_SIZE", JDBCType.INTEGER)
                .addColumn("NULLABLE", "NULLABLE", JDBCType.INTEGER)
                .addColumn("CASE_SENSITIVE", "CASE_SENSITIVE", JDBCType.BOOLEAN)
                .addColumn("SEARCHABLE", "SEARCHABLE", JDBCType.INTEGER)
                .addColumn("UNSIGNED_ATTRIBUTE", "UNSIGNED_ATTRIBUTE", JDBCType.BOOLEAN)
                .addColumn("FIXED_PREC_SCALE", "FIXED_PREC_SCALE", JDBCType.BOOLEAN)
                .addColumn("AUTO_INCREMENT", "AUTO_INCREMENT", JDBCType.BOOLEAN)
                .addColumn("LOCAL_TYPE_NAME", "LOCAL_TYPE_NAME", JDBCType.VARCHAR)
                .addColumn("MINIMUM_SCALE", "MINIMUM_SCALE", JDBCType.INTEGER)
                .addColumn("MAXIMUM_SCALE", "MAXIMUM_SCALE", JDBCType.INTEGER)
                .addColumn("SQL_DATA_TYPE", "SQL_DATA_TYPE", JDBCType.INTEGER)
                .addColumn("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", JDBCType.INTEGER)
                .addColumn("NUM_PREC_RADIX", "NUM_PREC_RADIX", JDBCType.INTEGER);



        List<Map<String, Object>> rows = Arrays.stream(WDFieldType.values())
                .map(fieldType -> {
                    Map<String, Object> data = new HashMap<>();

                    data.put("TYPE_NAME", fieldType.getJdbcType().getName());
                    data.put("DATA_TYPE", fieldType.getJdbcType().getVendorTypeNumber());
                    data.put("PRECISION", fieldType.getPrecision());
                    data.put("LITERAL_PREFIX", fieldType.getLiteralPrefix());
                    data.put("LITERAL_SUFFIX", fieldType.getLiteralSuffix());
                    // data.put("CREATE_PARAMS", );
                    // data.put("COLUMN_SIZE", );
                    data.put("NULLABLE", typeNullable);
                    data.put("CASE_SENSITIVE", fieldType.isCaseSensitive());
                    data.put("SEARCHABLE", typePredNone);
                    data.put("UNSIGNED_ATTRIBUTE", fieldType.isUnsigned());
                    data.put("FIXED_PREC_SCALE", false);
                    data.put("AUTO_INCREMENT", false);
                    data.put("LOCAL_TYPE_NAME", fieldType.getJdbcType().getName());
                    // data.put("MINIMUM_SCALE", );
                    // data.put("MAXIMUM_SCALE", );
                    // data.put("SQL_DATA_TYPE", );
                    // data.put("SQL_DATETIME_SUB", );
                    data.put("NUM_PREC_RADIX", 10);

                    return data;
                })
                .collect(Collectors.toList());

        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //TODO
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        //TODO
        return null;
    }

    private Stream<Triple<WDSchemaInfo, WDTableInfo, Pair<Integer, WDColumnInfo>>> streamOfColumn(Pair<WDSchemaInfo, WDTableInfo> entry, String columnNamePattern) {
        WDSchemaInfo schema = entry.getKey();
        WDTableInfo table = entry.getValue();
        Stream<ImmutablePair<Integer, WDColumnInfo>> columnInfoStream = IntStream.range(0, table.getColumns().size())
                .mapToObj(columnIdx -> new ImmutablePair<>(columnIdx + 1, table.getColumns().get(columnIdx)));
        if (columnNamePattern != null && !columnNamePattern.equals("%")) {
            columnInfoStream = columnInfoStream.filter(column -> columnNamePattern.equals(column.getRight().getColumnName()));
        }
        return columnInfoStream.map(column -> new ImmutableTriple<>(schema, table, column));
    }

    /**
     * @param catalog Ignore
     * @param schemaPattern Exact Match Only. If null, all match
     * @param tableNamePattern Exact Match Only. If null, all match
     * @param columnNamePattern Exact Match Only. If null, all match
     * @return
     * @throws SQLException
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

         SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                 .tableName("tables")
                 .addColumn("TABLE_CAT", "TABLE_CAT", JDBCType.VARCHAR)
                 .addColumn("TABLE_SCHEM", "TABLE_SCHEM", JDBCType.VARCHAR)
                 .addColumn("TABLE_NAME", "TABLE_NAME", JDBCType.VARCHAR)
                 .addColumn("COLUMN_NAME", "COLUMN_NAME", JDBCType.VARCHAR)
                 .addColumn("DATA_TYPE", "DATA_TYPE", JDBCType.VARCHAR)
                 .addColumn("TYPE_NAME", "TYPE_NAME", JDBCType.VARCHAR)
                 .addColumn("COLUMN_SIZE", "COLUMN_SIZE", JDBCType.INTEGER)
                 .addColumn("BUFFER_LENGTH", "BUFFER_LENGTH", JDBCType.INTEGER)
                 .addColumn("DECIMAL_DIGITS", "DECIMAL_DIGITS", JDBCType.VARCHAR)
                 .addColumn("NUM_PREC_RADIX", "NUM_PREC_RADIX", JDBCType.VARCHAR)
                 .addColumn("NULLABLE", "NULLABLE", JDBCType.INTEGER)
                 .addColumn("REMARKS", "REMARKS", JDBCType.VARCHAR)
                 .addColumn("COLUMN_DEF", "COLUMN_DEF", JDBCType.VARCHAR)
                 .addColumn("SQL_DATA_TYPE", "SQL_DATA_TYPE", JDBCType.INTEGER)
                 .addColumn("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", JDBCType.INTEGER)
                 .addColumn("CHAR_OCTET_LENGTH", "CHAR_OCTET_LENGTH", JDBCType.INTEGER)
                 .addColumn("ORDINAL_POSITION", "ORDINAL_POSITION", JDBCType.INTEGER)
                 .addColumn("IS_NULLABLE", "IS_NULLABLE", JDBCType.VARCHAR)
                 .addColumn("SCOPE_CATALOG", "SCOPE_CATALOG", JDBCType.VARCHAR)
                 .addColumn("SCOPE_SCHEMA", "SCOPE_SCHEMA", JDBCType.VARCHAR)
                 .addColumn("SCOPE_TABLE", "SCOPE_TABLE", JDBCType.VARCHAR)
                 .addColumn("SOURCE_DATA_TYPE", "SOURCE_DATA_TYPE", JDBCType.INTEGER)
                 .addColumn("IS_AUTOINCREMENT", "IS_AUTOINCREMENT", JDBCType.VARCHAR)
                 .addColumn("IS_GENERATEDCOLUMN", "IS_GENERATEDCOLUMN", JDBCType.VARCHAR);

         final String catalogName = this.connection.getCatalog();
         final int maxColumnNameLength = getMaxColumnNameLength();

         final List<Map<String, Object>> rows = streamOfSchema(schemaPattern)
                 .flatMap((schema) -> streamOfTable(schema, tableNamePattern))
                 .flatMap((entry) -> streamOfColumn(entry, columnNamePattern))
                 .map((entry) -> {
                     WDSchemaInfo schema = entry.getLeft();
                     WDTableInfo table = entry.getMiddle();
                     WDColumnInfo column = entry.getRight().getRight();
                     Map<String, Object> data = new HashMap<>();

                     data.put("TABLE_CAT", catalogName);
                     data.put("TABLE_SCHEM", schema.getSchemaName());
                     data.put("TABLE_NAME", table.getTableName());
                     data.put("COLUMN_NAME", column.getColumnName());
                     data.put("DATA_TYPE", column.getFieldType().getJdbcType().getVendorTypeNumber());
                     data.put("TYPE_NAME", column.getFieldType().getJdbcType().getName());
                     data.put("COLUMN_SIZE", maxColumnNameLength);
                     // data.put("BUFFER_LENGTH", );
                     // data.put("DECIMAL_DIGITS", );
                     data.put("NUM_PREC_RADIX", 10);
                     data.put("NULLABLE", typePredNone);
                     data.put("REMARKS", "");
                     data.put("COLUMN_DEF", "");
                     data.put("SQL_DATA_TYPE", column.getFieldType().getJdbcType().getVendorTypeNumber());
                     // data.put("SQL_DATETIME_SUB", );
                     // data.put("CHAR_OCTET_LENGTH", );
                     data.put("ORDINAL_POSITION", entry.getRight().getLeft());
                     data.put("IS_NULLABLE", "YES");
                     // data.put("SCOPE_CATALOG", );
                     // data.put("SCOPE_SCHEMA", );
                     // data.put("SCOPE_TABLE", );
                     // data.put("SOURCE_DATA_TYPE", );
                     data.put("IS_AUTOINCREMENT", false);
                     data.put("IS_GENERATEDCOLUMN", false);

                     return data;
                 })
                 .collect(Collectors.toList());
        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schemaName, String tableName) throws SQLException {

        SimpleResultSetMetaData.Builder builder = new SimpleResultSetMetaData.Builder()
                .tableName("primary_keys")
                .addColumn("TABLE_CAT", "TABLE_CAT", JDBCType.VARCHAR)
                .addColumn("TABLE_SCHEM", "TABLE_SCHEM", JDBCType.VARCHAR)
                .addColumn("TABLE_NAME", "TABLE_NAME", JDBCType.VARCHAR)
                .addColumn("COLUMN_NAME", "COLUMN_NAME", JDBCType.VARCHAR)
                .addColumn("KEY_SEQ", "KEY_SEQ", JDBCType.SMALLINT)
                .addColumn("PK_NAME", "PK_NAME", JDBCType.VARCHAR);

        final String catalogName = this.connection.getCatalog();
        final List<Triple<WDSchemaInfo, WDTableInfo, Pair<Integer, WDColumnInfo>>> columns =
                streamOfSchema(schemaName)
                        .flatMap((schema) -> streamOfTable(schema, tableName))
                        .flatMap((entry) -> streamOfColumn(entry, null))
                        .filter((entry) -> entry.getRight().getRight().getIsPrimary())
                        .collect(Collectors.toList());

        final List<Map<String, Object>> rows = IntStream
                .range(0, columns.size())
                .mapToObj((index) -> {
                    Triple<WDSchemaInfo, WDTableInfo, Pair<Integer, WDColumnInfo>> entry = columns.get(index);
                    WDSchemaInfo schema = entry.getLeft();
                    WDTableInfo table = entry.getMiddle();
                    WDColumnInfo column = entry.getRight().getRight();
                    Map<String, Object> data = new HashMap<>();

                    data.put("TABLE_CAT", catalogName);
                    data.put("TABLE_SCHEM", schema.getSchemaName());
                    data.put("TABLE_NAME", table.getTableName());
                    data.put("COLUMN_NAME", column.getColumnLabel());
                    data.put("DATA_TYPE", column.getFieldType().getJdbcType().getVendorTypeNumber());
                    data.put("KEY_SEQ", index + 1);
                    return data;
                })
                .collect(Collectors.toList());
        return new SimpleResultSet(builder, this.connection.createStatement(), rows);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        //TODO
        return null;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        //TODO
        return null;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }


    /**
     * Not supported methods
     */

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
