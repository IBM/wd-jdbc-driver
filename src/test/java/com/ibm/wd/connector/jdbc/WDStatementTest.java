package com.ibm.wd.connector.jdbc;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_CURSOR_KEY_FIELD_PATH;
import static com.ibm.wd.connector.jdbc.support.WDQueryIterator.generateCursorFilterString;

import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.watson.discovery.v2.model.QueryOptions;
import com.ibm.wd.connector.jdbc.support.WDQueryIterator;
import com.ibm.wd.connector.jdbc.utils.DiscoveryV2ForTestFactory;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class WDStatementTest {

    private DiscoveryV2ForTestFactory.Builder builder;

    private WDConnection getConnection(Properties properties) throws IOException, SQLException {
        if (builder == null) {
            QueryOptions.Builder queryBuilder = WDQueryIterator.generateBaseQueryOptsBuilder(
                    "e6402476-1bbb-4971-9839-c062e593a7b3",
                    "8357c1c6-1742-24c1-0000-018d1123e258",
                    10,
                    "metadata.cursor");
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
                            "collection2.json")
                    .importQueries(
                            queryBuilder
                                    .count(10)
                                    .filter(generateCursorFilterString(
                                            "metadata.cursor", Long.MIN_VALUE))
                                    .build(),
                            "queries.json")
                    .importQueries(
                            queryBuilder
                                    .count(10)
                                    .filter(generateCursorFilterString("metadata.cursor", 3))
                                    .build(),
                            "queries_none.json")
                    .importQueries(
                            queryBuilder
                                    .count(3)
                                    .filter(generateCursorFilterString(
                                            "metadata.cursor", Long.MIN_VALUE))
                                    .build(),
                            "queries.json")
                    .importQueries(
                            queryBuilder
                                    .count(3)
                                    .filter(generateCursorFilterString("metadata.cursor", 3))
                                    .build(),
                            "queries_none.json")
                    .importQueries(
                            queryBuilder
                                    .count(1)
                                    .filter(generateCursorFilterString(
                                            "metadata.cursor", Long.MIN_VALUE))
                                    .build(),
                            "queries_0.json")
                    .importQueries(
                            queryBuilder
                                    .count(1)
                                    .filter(generateCursorFilterString("metadata.cursor", 1))
                                    .build(),
                            "queries_1.json")
                    .importQueries(
                            queryBuilder
                                    .count(1)
                                    .filter(generateCursorFilterString("metadata.cursor", 2))
                                    .build(),
                            "queries_2.json")
                    .importQueries(
                            queryBuilder
                                    .count(1)
                                    .filter(generateCursorFilterString("metadata.cursor", 3))
                                    .build(),
                            "queries_none.json")
                    .importQueries(
                            queryBuilder
                                    .count(2)
                                    .filter(generateCursorFilterString(
                                            "metadata.cursor", Long.MIN_VALUE))
                                    .build(),
                            "queries_0-1.json")
                    .importQueries(
                            queryBuilder
                                    .count(2)
                                    .filter(generateCursorFilterString("metadata.cursor", 2))
                                    .build(),
                            "queries_2.json")
                    .importQueries(
                            queryBuilder
                                    .count(2)
                                    .filter(generateCursorFilterString("metadata.cursor", 3))
                                    .build(),
                            "queries_none.json");
        }
        return new WDConnection(builder.build(), properties);
    }

    @Test
    public void testFetchAllEnriched() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        try (WDConnection connection = getConnection(properties)) {
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery(
                    "select " + "\"parent_document_id\", "
                            + "\"document_id\", "
                            + "\"wd_field_path\", "
                            + "\"enriched_html_wd_nested_seq_num\", "
                            + "\"enriched_html_tables_wd_nested_seq_num\", "
                            + "\"enriched_html_tables_body_cells_wd_nested_seq_num\", "
                            + "\"enriched_html_tables_body_cells_cell_id\", "
                            + "\"enriched_html_tables_body_cells_column_header_ids\", "
                            + "\"enriched_html_tables_body_cells_column_header_texts\", "
                            + "\"enriched_html_tables_body_cells_column_header_texts_normalized\", "
                            + "\"enriched_html_tables_body_cells_column_index_begin\", "
                            + "\"enriched_html_tables_body_cells_column_index_end\", "
                            + "\"enriched_html_tables_body_cells_location_begin\", "
                            + "\"enriched_html_tables_body_cells_location_end\", "
                            + "\"enriched_html_tables_body_cells_row_header_ids\", "
                            + "\"enriched_html_tables_body_cells_row_header_texts\", "
                            + "\"enriched_html_tables_body_cells_row_header_texts_normalized\", "
                            + "\"enriched_html_tables_body_cells_row_index_begin\", "
                            + "\"enriched_html_tables_body_cells_row_index_end\", "
                            + "\"enriched_html_tables_body_cells_text\" "
                            + "from \"Test CI Project\".\"Annual Report Collection 1 [tables from enriched_html]\"");

            int numberOfFirstDoc = 0;
            String firstDocId = "83a656a5d241d1a9715b50ec80cb482a";
            int numberOfSecondDoc = 0;
            String secondDocId = "00467b5ca6a18cc0cfa9ac2b8918bf53";
            int numberOfThirdDoc = 0;
            String thirdDocId = "7c1089f61e0cea6c98b99f6492b0bcd8";
            while (result.next()) {
                if (result.getString("parent_document_id").equals(firstDocId)
                        && result.getString("document_id").equals(firstDocId)) {
                    numberOfFirstDoc++;
                }
                if (result.getString("parent_document_id").equals(secondDocId)
                        && result.getString("document_id").equals(secondDocId)) {
                    numberOfSecondDoc++;
                }
                if (result.getString("parent_document_id").equals(thirdDocId)
                        && result.getString("document_id").equals(thirdDocId)) {
                    numberOfThirdDoc++;
                }
                String resultString = result.getString("parent_document_id")
                        + " " + result.getString("document_id")
                        + " " + result.getString("wd_field_path")
                        + " " + result.getString("enriched_html_wd_nested_seq_num")
                        + " " + result.getString("enriched_html_tables_wd_nested_seq_num")
                        + " "
                        + result.getString("enriched_html_tables_body_cells_wd_nested_seq_num")
                        + " " + result.getString("enriched_html_tables_body_cells_cell_id")
                        + " "
                        + result.getString("enriched_html_tables_body_cells_column_header_ids")
                        + " "
                        + result.getString("enriched_html_tables_body_cells_column_header_texts")
                        + " "
                        + result.getString(
                                "enriched_html_tables_body_cells_column_header_texts_normalized")
                        + " "
                        + result.getShort("enriched_html_tables_body_cells_column_index_begin")
                        + " " + result.getInt("enriched_html_tables_body_cells_column_index_end")
                        + " " + result.getLong("enriched_html_tables_body_cells_location_begin")
                        + " " + result.getBigDecimal("enriched_html_tables_body_cells_location_end")
                        + " " + result.getString("enriched_html_tables_body_cells_row_header_ids")
                        + " " + result.getString("enriched_html_tables_body_cells_row_header_texts")
                        + " "
                        + result.getString(
                                "enriched_html_tables_body_cells_row_header_texts_normalized")
                        + " " + result.getString("enriched_html_tables_body_cells_row_index_begin")
                        + " " + result.getString("enriched_html_tables_body_cells_row_index_end")
                        + " " + result.getString("enriched_html_tables_body_cells_text");
                Assertions.assertTrue(StringUtils.isNotBlank(resultString));
            }
            Assertions.assertEquals(34, numberOfFirstDoc);
            Assertions.assertEquals(52, numberOfSecondDoc);
            Assertions.assertEquals(32, numberOfThirdDoc);
        }
    }

    @Test
    public void testFetchAll() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        try (WDConnection connection = getConnection(properties)) {
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery("select " + "\"parent_document_id\", "
                    + "\"document_id\", "
                    + "\"wd_field_path\", "
                    + "\"text\" "
                    + "from \"Test CI Project\".\"Annual Report Collection 1 [wd_root_doc]\"");

            int numberOfFirstDoc = 0;
            String firstDocId = "83a656a5d241d1a9715b50ec80cb482a";
            int numberOfSecondDoc = 0;
            String secondDocId = "00467b5ca6a18cc0cfa9ac2b8918bf53";
            int numberOfThirdDoc = 0;
            String thirdDocId = "7c1089f61e0cea6c98b99f6492b0bcd8";
            while (result.next()) {
                if (result.getString("parent_document_id").equals(firstDocId)
                        && result.getString("document_id").equals(firstDocId)) {
                    numberOfFirstDoc++;
                }
                if (result.getString("parent_document_id").equals(secondDocId)
                        && result.getString("document_id").equals(secondDocId)) {
                    numberOfSecondDoc++;
                }
                if (result.getString("parent_document_id").equals(thirdDocId)
                        && result.getString("document_id").equals(thirdDocId)) {
                    numberOfThirdDoc++;
                }
                String resultString = result.getString("parent_document_id")
                        + " " + result.getString("document_id")
                        + " " + result.getString("wd_field_path")
                        + " " + result.getString("text");
                Assertions.assertTrue(StringUtils.isNotBlank(resultString));
            }
            Assertions.assertEquals(1, numberOfFirstDoc);
            Assertions.assertEquals(1, numberOfSecondDoc);
            Assertions.assertEquals(1, numberOfThirdDoc);
        }
    }

    @Test
    public void testErrorIfAccessingNonExistingColumn() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        try (WDConnection connection = getConnection(properties)) {
            Statement stmt = connection.createStatement();
            ResultSet result = stmt.executeQuery(
                    "select \"parent__document__id\" from \"Test CI Project\".\"Annual Report Collection 1 [tables from enriched_html]\"");
            Assertions.assertThrows(SQLException.class, () -> {
                result.getString("parent_document_id");
            });
        }
    }
}
