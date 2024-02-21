package com.ibm.wd.connector.jdbc.support;

import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.watson.discovery.v2.model.*;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class WDSimpleClient implements WDClientInterface {

    private Discovery client;

    public WDSimpleClient(Discovery client) {
        this.client = client;
    }

    public List<ProjectListDetails> listProjects() throws SQLException {
        Response<ListProjectsResponse> response = client.listProjects().execute();
        if (response.getStatusCode() != 200) {
            throw handleErrorResponse(response, "list projects");
        }
        return response.getResult().getProjects();
    }

    public List<Field> listFields(String projectId) throws SQLException {

        if (listCollections(projectId).isEmpty()) {
            return Collections.emptyList();
        }

        Response<ListFieldsResponse> response =
                client.listFields(new ListFieldsOptions.Builder().projectId(projectId).build()).execute();
        if (response.getStatusCode() != 200) {
            throw handleErrorResponse(response, "list fields");
        }
        return response.getResult().getFields();
    }

    public List<Collection> listCollections(String projectId) throws SQLException {
        Response<ListCollectionsResponse> response =
                client.listCollections(new ListCollectionsOptions.Builder().projectId(projectId).build())
                        .execute();
        if (response.getStatusCode() != 200) {
            throw handleErrorResponse(response, "list collections");
        }
        return response.getResult().getCollections();
    }

    public CollectionDetails getCollection(String projectId, String collectionId) throws SQLException {

        GetCollectionOptions options = new GetCollectionOptions.Builder()
                .projectId(projectId)
                .collectionId(collectionId)
                .build();
        Response<CollectionDetails> response = client.getCollection(options).execute();
        if (response.getStatusCode() != 200) {
            throw handleErrorResponse(response, "get collection");
        }
        return response.getResult();
    }

    private <T> SQLException handleErrorResponse(Response<T> response, String method) {
        return new SQLException(
                "Unexpected response status for " + method + ". "
                        + "status_code=" + response.getStatusCode()
                        + " message=" + response.getStatusMessage());
    }
}
