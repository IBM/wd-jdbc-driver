package com.ibm.wd.connector.jdbc.support;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_CURSOR_KEY_FIELD_PATH;
import static com.ibm.wd.connector.jdbc.model.WDConstants.DISCOVERY_DOCUMENT_ID_FIELD_PATH;
import static com.ibm.wd.connector.jdbc.model.WDConstants.DISCOVERY_METADATA_FIELD_PATH;

import com.google.gson.Gson;
import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.watson.discovery.v2.model.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

public class WDQueryIterator implements Iterator<Map<String, Object>> {

    private final Discovery discovery;
    private final String sortKey; // should be long field, unique
    private final QueryOptions.Builder baseQueryOptsBuilder;

    private long currentCursorValue;
    private Iterator<QueryResult> results;

    private static final Gson gson = new Gson();

    public static QueryOptions.Builder generateBaseQueryOptsBuilder(
            String projectId, String collectionId, int fetchSize, String sortKey) {
        return new QueryOptions.Builder()
                .projectId(projectId)
                .addCollectionIds(collectionId)
                .aggregation("")
                .count(fetchSize)
                .offset(0)
                .spellingSuggestions(false)
                .highlight(false)
                .passages(new QueryLargePassages.Builder().enabled(false).build())
                .tableResults(
                        new QueryLargeTableResults.Builder().enabled(false).build())
                .similar(new QueryLargeSimilar.Builder().enabled(false).build())
                .sort(sortKey);
    }

    public static String generateCursorFilterString(String sortKey, long cursorValue) {
        return String.format("%s > %d", sortKey, cursorValue);
    }

    public WDQueryIterator(
            Discovery discovery,
            String projectId,
            String collectionId,
            Properties properties,
            int fetchSize) {
        this.discovery = discovery;
        this.sortKey = WD_CURSOR_KEY_FIELD_PATH.get(properties);
        this.currentCursorValue = Long.MIN_VALUE;
        this.baseQueryOptsBuilder =
                generateBaseQueryOptsBuilder(projectId, collectionId, fetchSize, sortKey);
        loadNext();
    }

    private QueryResponse query() throws RuntimeException {
        QueryOptions options = baseQueryOptsBuilder
                .filter(generateCursorFilterString(sortKey, currentCursorValue))
                .build();
        Response<QueryResponse> response = discovery.query(options).execute();
        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Unexpected response status for query. "
                    + "status_code=" + response.getStatusCode()
                    + " message=" + response.getStatusMessage());
        }
        return response.getResult();
    }

    private boolean loadNext() {
        QueryResponse response = query();
        if (response.getMatchingResults() == 0) {
            results = null;
        } else {
            results = response.getResults().iterator();
            int hitCounts = response.getResults().size();
            currentCursorValue = ((Number) WDDocValueExtractor.extractValue(
                            sortKey, convertQueryResult(response.getResults().get(hitCounts - 1))))
                    .longValue();
            if (!results.hasNext()) {
                results = null;
            }
        }
        return results != null && results.hasNext();
    }

    @Override
    public boolean hasNext() {
        return results.hasNext() || loadNext();
    }

    private Map<String, Object> convertQueryResult(QueryResult result) {
        Map<String, Object> object = new HashMap<>(result.getProperties());
        object.put(DISCOVERY_METADATA_FIELD_PATH, result.getMetadata());
        object.put(DISCOVERY_DOCUMENT_ID_FIELD_PATH, result.getDocumentId());
        return object;
    }

    private void dumpObject(Map<String, Object> object) {
        try {
            System.err.println(gson.toJson(object));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> next() {
        if (results == null) {
            throw new NoSuchElementException("there isn't results any more");
        }
        return convertQueryResult(results.next());
    }
}
