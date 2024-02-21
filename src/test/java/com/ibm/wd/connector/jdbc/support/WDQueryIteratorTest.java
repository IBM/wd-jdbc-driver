package com.ibm.wd.connector.jdbc.support;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_CURSOR_KEY_FIELD_PATH;
import static com.ibm.wd.connector.jdbc.support.WDQueryIterator.generateCursorFilterString;

import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.watson.discovery.v2.model.QueryOptions;
import com.ibm.wd.connector.jdbc.utils.DiscoveryV2ForTestFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class WDQueryIteratorTest {

    private Discovery discovery;

    private Discovery getDiscovery(Properties properties) throws IOException, SQLException {
        if (discovery == null) {
            QueryOptions.Builder queryBuilder = WDQueryIterator.generateBaseQueryOptsBuilder(
                    "e6402476-1bbb-4971-9839-c062e593a7b3",
                    "8357c1c6-1742-24c1-0000-018d1123e258",
                    10,
                    "metadata.cursor");
            discovery = new DiscoveryV2ForTestFactory.Builder()
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
                            "queries_none.json")
                    .build()
                    .create(properties);
        }
        return discovery;
    }

    @Test
    public void testWithFetchSize10() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        Discovery discovery = getDiscovery(properties);
        WDQueryIterator iterator = new WDQueryIterator(
                discovery,
                "e6402476-1bbb-4971-9839-c062e593a7b3",
                "8357c1c6-1742-24c1-0000-018d1123e258",
                properties,
                10);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assertions.assertEquals(3, count);
    }

    @Test
    public void testWithFetchSize3() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        Discovery discovery = getDiscovery(properties);
        WDQueryIterator iterator = new WDQueryIterator(
                discovery,
                "e6402476-1bbb-4971-9839-c062e593a7b3",
                "8357c1c6-1742-24c1-0000-018d1123e258",
                properties,
                3);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assertions.assertEquals(3, count);
    }

    @Test
    public void testWithFetchSize2() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        Discovery discovery = getDiscovery(properties);
        WDQueryIterator iterator = new WDQueryIterator(
                discovery,
                "e6402476-1bbb-4971-9839-c062e593a7b3",
                "8357c1c6-1742-24c1-0000-018d1123e258",
                properties,
                2);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assertions.assertEquals(3, count);
    }

    @Test
    public void testWithFetchSize1() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.setProperty(WD_CURSOR_KEY_FIELD_PATH.getName(), "metadata.cursor");
        Discovery discovery = getDiscovery(properties);
        WDQueryIterator iterator = new WDQueryIterator(
                discovery,
                "e6402476-1bbb-4971-9839-c062e593a7b3",
                "8357c1c6-1742-24c1-0000-018d1123e258",
                properties,
                1);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assertions.assertEquals(3, count);
    }
}
