package com.ibm.wd.connector.jdbc.support;

import com.ibm.watson.discovery.v2.model.QueryResult;
import com.ibm.wd.connector.jdbc.model.WDFieldPath;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.Map;

import static com.ibm.wd.connector.jdbc.model.WDConstants.DISCOVERY_FIELD_PATH_SEPARATOR_REGEX;

public class WDObjectIterator implements Iterator<Object> {

    private final WDFieldPath fieldPath;
    private final WDQueryIterator queryIterator;
    private NestedObjectIterator nestedObjectIterator;
    private NestedObjectIterator previousNestedObjectIterator;

    private final boolean generateNestedStrictly;


    public WDObjectIterator(WDQueryIterator queryIterator, WDFieldPath fieldPathToObjects, boolean generateNestedStrictly) {
        this.queryIterator = queryIterator;
        this.fieldPath = fieldPathToObjects;
        this.generateNestedStrictly = generateNestedStrictly;
        loadNextObjectIteratorIfRequired();
    }

    private void loadNextObjectIteratorIfRequired() {
        if (previousNestedObjectIterator != null) {
            previousNestedObjectIterator = null;
        }
        if ((nestedObjectIterator == null || !nestedObjectIterator.hasNext()) && queryIterator.hasNext()) {
            previousNestedObjectIterator = nestedObjectIterator;
            nestedObjectIterator = WDDocValueExtractor.generateNestedObjectIterator(
                    fieldPath.getFieldPath(),
                    queryIterator.next(),
                    generateNestedStrictly
            );
        }
    }

    @Override
    public boolean hasNext() {
        return nestedObjectIterator.hasNext();
    }

    @Override
    public Object next() {
        Object value = nestedObjectIterator.next();
        loadNextObjectIteratorIfRequired();
        return value;
    }

    private NestedObjectIterator getNestedIteratorHoldingCurrentSequence() {
        return previousNestedObjectIterator != null ? previousNestedObjectIterator : nestedObjectIterator;
    }

    public Pair<String, Object> getCurrentObjectHavingPath(WDFieldPath fieldPath) {
        return getNestedIteratorHoldingCurrentSequence()
                .getCurrentObjectHavingPath(fieldPath.getFieldPathComps());
    }

    public Pair<String, Integer> getCurrentSequenceNumberHavingPath(WDFieldPath fieldPath) {
        return getNestedIteratorHoldingCurrentSequence()
                .getCurrentSequenceNumberHavingPath(fieldPath.getFieldPathComps());
    }
}
