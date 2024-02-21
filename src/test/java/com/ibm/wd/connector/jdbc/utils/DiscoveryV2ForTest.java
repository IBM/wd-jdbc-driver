package com.ibm.wd.connector.jdbc.utils;

import com.ibm.cloud.sdk.core.http.ServiceCall;
import com.ibm.cloud.sdk.core.http.ServiceCallback;
import com.ibm.cloud.sdk.core.security.BasicAuthenticator;
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

import io.reactivex.Single;

import okhttp3.HttpUrl;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;

import java.util.Map;

public class DiscoveryV2ForTest extends Discovery {

    private final ListProjectsResponse listProjects;
    private final Map<ListFieldsOptions, ListFieldsResponse> listFields;
    private final Map<ListCollectionsOptions, ListCollectionsResponse> listCollections;
    private final Map<GetCollectionOptions, CollectionDetails> collectionDetails;
    private final Map<QueryOptions, QueryResponse> queries;

    public DiscoveryV2ForTest(
            String version,
            ListProjectsResponse listProjects,
            Map<ListFieldsOptions, ListFieldsResponse> listFields,
            Map<ListCollectionsOptions, ListCollectionsResponse> listCollections,
            Map<GetCollectionOptions, CollectionDetails> collectionDetails,
            Map<QueryOptions, QueryResponse> queries) {
        super(
                version,
                new BasicAuthenticator.Builder()
                        .username("dummy user")
                        .password("dummy password")
                        .build());
        this.listProjects = listProjects;
        this.listFields = listFields;
        this.listCollections = listCollections;
        this.collectionDetails = collectionDetails;
        this.queries = queries;
    }

    @Override
    public ServiceCall<ListProjectsResponse> listProjects() {
        return new ServiceCallForTest<>(listProjects);
    }

    @Override
    public ServiceCall<ListFieldsResponse> listFields(ListFieldsOptions listFieldsOptions) {
        return new ServiceCallForTest<>(
                listFields == null ? null : listFields.get(listFieldsOptions));
    }

    @Override
    public ServiceCall<ListCollectionsResponse> listCollections(
            ListCollectionsOptions listCollectionsOptions) {
        return new ServiceCallForTest<>(
                listCollections == null ? null : listCollections.get(listCollectionsOptions));
    }

    @Override
    public ServiceCall<CollectionDetails> getCollection(GetCollectionOptions getCollectionOptions) {
        return new ServiceCallForTest<>(
                collectionDetails == null ? null : collectionDetails.get(getCollectionOptions));
    }

    @Override
    public ServiceCall<QueryResponse> query(QueryOptions queryOptions) {
        return new ServiceCallForTest<>(queries == null ? null : queries.get(queryOptions));
    }

    /**
     * Defines implementation for modifying and executing service calls.
     *
     * @param <T> the generic type
     */
    public class ServiceCallForTest<T> implements ServiceCall<T> {
        private T response;

        ServiceCallForTest(T response) {
            this.response = response;
        }

        @Override
        public ServiceCall<T> addHeader(String name, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.ibm.cloud.sdk.core.http.Response<T> execute() {
            return new com.ibm.cloud.sdk.core.http.Response<>(
                    response,
                    new Response.Builder()
                            .code(200)
                            .protocol(Protocol.HTTP_1_1)
                            .message("dummy")
                            .request(new Request.Builder()
                                    .url(new HttpUrl.Builder()
                                            .scheme("http")
                                            .host("dummy")
                                            .build())
                                    .build())
                            .build());
        }

        @Override
        public void enqueue(final ServiceCallback<T> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Single<com.ibm.cloud.sdk.core.http.Response<T>> reactiveRequest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() throws Throwable {
            throw new UnsupportedOperationException();
        }
    }
}
