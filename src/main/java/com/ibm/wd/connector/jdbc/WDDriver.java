package com.ibm.wd.connector.jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WDDriver implements Driver {
    private static final DriverPropertyInfo[] DRIVER_PROPERTY_INFO = new DriverPropertyInfo[0];
    private static final Pattern JDBC_URL_PATTERN = Pattern.compile("jdbc:wd://(.*)");

    static {
        try {
            java.sql.DriverManager.registerDriver(new WDDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        urlNullCheck(url);
        final Matcher matcher = JDBC_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            return null;
        }

        String wdServiceUrl = matcher.group(1);
        return new WDConnection(new DiscoveryV2Factory(wdServiceUrl), info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        urlNullCheck(url);
        final Matcher matcher = JDBC_URL_PATTERN.matcher(url);
        return matcher.matches();
    }

    private void urlNullCheck(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("url is null");
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return DRIVER_PROPERTY_INFO;
    }

    @Override
    public int getMajorVersion() {
        return WDDriver.getDriverMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return WDDriver.getDriverMinorVersion();
    }

    public static int getDriverMajorVersion() {
        final String version = WDDriver.class.getPackage().getImplementationVersion();
        if (version == null) {
            return 0;
        }
        final String[] splits = version.split("\\.");
        if ((splits == null) || (splits.length < 1)) {
            return 0;
        }
        return Integer.parseInt(splits[0]);
    }

    public static int getDriverMinorVersion() {
        final String version = WDDriver.class.getPackage().getImplementationVersion();
        if (version == null) {
            return 0;
        }
        final String[] splits = version.split("\\.");
        if ((splits == null) || (splits.length < 2)) {
            return 0;
        }
        return Integer.parseInt(splits[splits.length - 1]);
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private static void dumpSchemasAndTables(
            String jdbcUrl, String schemaName, String tableName, Properties props)
            throws Exception {

        try (Connection connection = new WDDriver().connect(jdbcUrl, props)) {
            ResultSet schemas = connection.getMetaData().getSchemas(null, schemaName);
            while (schemas.next()) {
                String schema = schemas.getString("TABLE_SCHEM");
                System.out.println("schema: " + schema);
                ResultSet tables =
                        connection.getMetaData().getTables(null, schema, tableName, null);
                while (tables.next()) {
                    String table = tables.getString("TABLE_NAME");
                    System.out.println("\ttable: " + table);
                    ResultSet columns =
                            connection.getMetaData().getColumns(null, schema, table, null);
                    while (columns.next()) {
                        System.out.println("\t\t column: "
                                + columns.getString("COLUMN_NAME")
                                + " " + columns.getString("TYPE_NAME"));
                    }
                }
            }
        }
    }
}
