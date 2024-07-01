package com.ibm.wd.connector.jdbc.sql;

import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.wd.connector.jdbc.WDConnection;
import com.ibm.wd.connector.jdbc.utils.DiscoveryV2ForTestFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WDInstanceMetaDataTest {

    private DiscoveryV2ForTestFactory.Builder builder;

    private WDConnection getConnection(Properties properties) throws IOException, SQLException {
        if (builder == null) {
            builder = new DiscoveryV2ForTestFactory.Builder()
                    .importListProjects("projects.json")
                    .importListCollections(
                            new ListCollectionsOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .build(),
                            "collections.json")
                    .importListFields(
                            new ListFieldsOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .build(),
                            "fields.json")
                    .importCollectionDetails(
                            new GetCollectionOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .collectionId("8357c1c6-1742-24c1-0000-018d1123e258")
                                    .build(),
                            "collection1.json")
                    .importCollectionDetails(
                            new GetCollectionOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .collectionId("7357c1c6-1742-24c1-0000-018d1123e258")
                                    .build(),
                            "collection2.json");
        }
        return new WDConnection(builder.build(), properties);
    }

    @Test
    public void test() throws SQLException, IOException {
        Properties properties = new Properties();
        try (WDConnection connection = getConnection(properties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            Assertions.assertNotNull(metadata);
            metadata.getCatalogs();

            String schema = "Test CI Project";
            String table = "Annual Report Collection 1 [tables from enriched_html]";

            ResultSet schemas = connection.getMetaData().getSchemas(null, schema);
            while (schemas.next()) {
                Assertions.assertEquals(schema, schemas.getString("TABLE_SCHEM"));
                ResultSet tables = connection.getMetaData().getTables(null, schema, table, null);
                while (tables.next()) {
                    Assertions.assertEquals(table, tables.getString("TABLE_NAME"));
                    ResultSet columnResultSet = metadata.getColumns(null, schema, table, null);
                    List<String> columnNames = new ArrayList<>();
                    while (columnResultSet.next()) {
                        String columnName = columnResultSet.getString("COLUMN_NAME");
                        columnNames.add(columnName);
                    }
                    Assertions.assertEquals(20, columnNames.size());
                    Assertions.assertEquals("parent_document_id", columnNames.get(0));
                }
            }
        }
    }
}
