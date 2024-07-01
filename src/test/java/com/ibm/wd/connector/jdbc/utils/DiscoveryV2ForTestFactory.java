package com.ibm.wd.connector.jdbc.utils;

import com.google.gson.stream.JsonReader;
import com.ibm.cloud.sdk.core.util.GsonSingleton;
import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.watson.discovery.v2.model.CollectionDetails;
import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsResponse;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsResponse;
import com.ibm.watson.discovery.v2.model.ListProjectsResponse;
import com.ibm.watson.discovery.v2.model.QueryOptions;
import com.ibm.watson.discovery.v2.model.QueryResponse;
import com.ibm.wd.connector.jdbc.DiscoveryV2Factory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DiscoveryV2ForTestFactory extends DiscoveryV2Factory {

    private ListProjectsResponse listProjects;
    private Map<ListFieldsOptions, ListFieldsResponse> listFields;
    private Map<ListCollectionsOptions, ListCollectionsResponse> listCollections;
    private Map<GetCollectionOptions, CollectionDetails> collectionDetails;
    private Map<QueryOptions, QueryResponse> queries;

    private DiscoveryV2ForTestFactory() {
        super("dummy");
        this.listFields = new HashMap<>();
        this.listCollections = new HashMap<>();
        this.collectionDetails = new HashMap<>();
        this.queries = new HashMap<>();
    }

    @Override
    public Discovery create(Properties properties) throws SQLException {
        return new DiscoveryV2ForTest(
                "1111-11-11",
                listProjects,
                listFields,
                listCollections,
                collectionDetails,
                queries);
    }

    public static class Builder {
        private final DiscoveryV2ForTestFactory factory = new DiscoveryV2ForTestFactory();

        private <T> T getValue(Reader json, com.google.gson.reflect.TypeToken<T> typeToken) {
            JsonReader reader;
            reader = new JsonReader(json);
            return GsonSingleton.getGsonWithoutPrettyPrinting()
                    .fromJson(reader, typeToken.getType());
        }

        public Builder importListProjects(String path) throws IOException {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
                Reader reader = new InputStreamReader(stream);
                factory.listProjects = getValue(
                        reader, new com.google.gson.reflect.TypeToken<ListProjectsResponse>() {});
            }
            return this;
        }

        public Builder importListFields(ListFieldsOptions options, String path) throws IOException {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
                Reader reader = new InputStreamReader(stream);
                factory.listFields.put(
                        options,
                        getValue(
                                reader,
                                new com.google.gson.reflect.TypeToken<ListFieldsResponse>() {}));
            }
            return this;
        }

        public Builder importListCollections(ListCollectionsOptions options, String path)
                throws IOException {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
                Reader reader = new InputStreamReader(stream);
                factory.listCollections.put(
                        options,
                        getValue(
                                reader,
                                new com.google.gson.reflect.TypeToken<
                                        ListCollectionsResponse>() {}));
            }
            return this;
        }

        public Builder importCollectionDetails(GetCollectionOptions options, String path)
                throws IOException {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
                Reader reader = new InputStreamReader(stream);
                factory.collectionDetails.put(
                        options,
                        getValue(
                                reader,
                                new com.google.gson.reflect.TypeToken<CollectionDetails>() {}));
            }
            return this;
        }

        public Builder importQueries(QueryOptions options, String path) throws IOException {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
                Reader reader = new InputStreamReader(stream);
                factory.queries.put(
                        options,
                        getValue(
                                reader, new com.google.gson.reflect.TypeToken<QueryResponse>() {}));
            }
            return this;
        }

        public DiscoveryV2Factory build() {
            return factory;
        }
    }
}
