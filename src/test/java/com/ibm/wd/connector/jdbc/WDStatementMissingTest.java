package com.ibm.wd.connector.jdbc;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_CURSOR_KEY_FIELD_PATH;
import static com.ibm.wd.connector.jdbc.WDProperties.WD_DEFAULT_FETCH_SIZE;
import static com.ibm.wd.connector.jdbc.support.WDQueryIterator.generateCursorFilterString;

import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.watson.discovery.v2.model.QueryOptions;
import com.ibm.wd.connector.jdbc.support.WDQueryIterator;
import com.ibm.wd.connector.jdbc.utils.DiscoveryV2ForTestFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class WDStatementMissingTest {

    private DiscoveryV2ForTestFactory.Builder builder;

    private WDConnection getConnection(Properties properties) throws IOException, SQLException {
        if (builder == null) {
            QueryOptions.Builder queryBuilder = WDQueryIterator.generateBaseQueryOptsBuilder(
                    "2279f8ca-ec68-4921-b90a-3ffdb65e6bbc",
                    "0e7b5d0f-f98f-4bf9-0000-018e807cf40a",
                    3,
                    "metadata.wd_cursor_key");
            builder = new DiscoveryV2ForTestFactory.Builder()
                    .importListProjects("projects_missing.json")
                    .importListCollections(
                            new ListCollectionsOptions.Builder()
                                    .projectId("2279f8ca-ec68-4921-b90a-3ffdb65e6bbc")
                                    .build(),
                            "collections_missing.json")
                    .importListFields(
                            new ListFieldsOptions.Builder()
                                    .projectId("2279f8ca-ec68-4921-b90a-3ffdb65e6bbc")
                                    .build(),
                            "fields_missing.json")
                    .importCollectionDetails(
                            new GetCollectionOptions.Builder()
                                    .projectId("2279f8ca-ec68-4921-b90a-3ffdb65e6bbc")
                                    .collectionId("0e7b5d0f-f98f-4bf9-0000-018e807cf40a")
                                    .build(),
                            "collection1_missing.json")
                    .importQueries(
                            queryBuilder
                                    .count(3)
                                    .filter(generateCursorFilterString(
                                            "metadata.wd_cursor_key", Long.MIN_VALUE))
                                    .build(),
                            "queries_missing_0.json")
                    .importQueries(
                            queryBuilder
                                    .count(3)
                                    .filter(generateCursorFilterString("metadata.wd_cursor_key", 2))
                                    .build(),
                            "queries_missing_1.json")
                    .importQueries(
                            queryBuilder
                                    .count(3)
                                    .filter(generateCursorFilterString("metadata.wd_cursor_key", 5))
                                    .build(),
                            "queries_none.json");
        }
        return new WDConnection(builder.build(), properties);
    }

    @Test
    public void testIteratingMissingObjects() throws IOException, SQLException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.wd_cursor_key");
        properties.setProperty(WD_DEFAULT_FETCH_SIZE.getName(), "3");
        int count = 0;
        double[] expected = new double[] {0.7244338, 0.8};
        try (WDConnection connection = getConnection(properties)) {
            Statement stmt = connection.createStatement();

            ResultSet result = stmt.executeQuery(
                    "select \"enriched_text_entities_mentions_confidence\" from \"Driver Test\".\"Driver Test Collection 1 [entities from enriched_text]\"");
            while (result.next()) {
                Assertions.assertEquals(
                        expected[count],
                        result.getDouble("enriched_text_entities_mentions_confidence"));
                count++;
            }
        }
        Assertions.assertEquals(expected.length, count);
    }
}
