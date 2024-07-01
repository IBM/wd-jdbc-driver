package com.ibm.wd.connector.jdbc.support;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.ibm.wd.connector.jdbc.model.WDConstants.*;
import static com.ibm.wd.connector.jdbc.model.WDFieldPath.ROOT_DOC_PATH;
import static com.ibm.wd.connector.jdbc.support.NestedObjectIterator.EMPTY_ITERATOR;

public class WDDocValueExtractor {

    // drop initial "." if exists
    private static String normalizeFieldPath(String fieldPath) {
        if (fieldPath.startsWith(".")) {
            return fieldPath.substring(1);
        } else {
            return fieldPath;
        }
    }

    @SuppressWarnings("unchecked")
    public static Object extractValue(String fieldPath, Map<String, ?> properties) {
        if (fieldPath == null) {
            throw new IllegalArgumentException("Field path should not be null");
        }
        fieldPath = normalizeFieldPath(fieldPath);

        PathCompSlice slice = new PathCompSlice(fieldPath, DISCOVERY_FIELD_PATH_SEPARATOR_REGEX);
        Iterator<Pair<String, Integer>> pathCompItr = slice.iterator();
        while (pathCompItr.hasNext()) {
            Pair<String, Integer> pathCompIdx = pathCompItr.next();
            String pathComp = pathCompIdx.getLeft();
            Integer pathIndex = pathCompIdx.getRight();
            if (pathComp.equals(ROOT_DOC_PATH.getFieldPath())) {
                continue;
            }
            Object value = properties.get(pathComp);
            if (slice.hasNextComp(pathIndex + 1)) {
                if (value instanceof Map) {
                    properties = (Map<String, ?>)value;
                } else {
                    throw new IllegalArgumentException("Field path terminated at " + slice.pathStringUntil(pathIndex, DISCOVERY_FIELD_PATH_SEPARATOR));
                }
            } else {
                return value;
            }
        }
        throw new IllegalArgumentException("Could not find value in field path: " + fieldPath);

    }

    public static NestedObjectIterator generateNestedObjectIterator(String fieldPath, Map<String, ?> properties, boolean strictly) {
        fieldPath = normalizeFieldPath(fieldPath);
        PathCompSlice slice = new PathCompSlice(fieldPath, DISCOVERY_FIELD_PATH_SEPARATOR_REGEX);
        return generateNestedObjectIterator(slice, properties, strictly);
    }

    @SuppressWarnings("unchecked")
    public static NestedObjectIterator generateNestedObjectIterator(PathCompSlice slice, Map<String, ?> properties, boolean strictly) {

        Iterator<Pair<String, Integer>> pathCompItr = slice.iterator();

        while (pathCompItr.hasNext()) {
            Pair<String, Integer> pathCompIdx = pathCompItr.next();
            String pathComp = pathCompIdx.getLeft();
            Integer pathIndex = pathCompIdx.getRight();
            if (pathComp.equals(ROOT_DOC_PATH.getFieldPath())) {
                return new NestedObjectIterator(
                        slice.from(pathIndex + 1),
                        slice.pathString(DISCOVERY_FIELD_PATH_SEPARATOR),
                        new SingleOrMultiObjectIterator<>(properties),
                        strictly
                );
            }
            Object value = properties.get(pathComp);
            if (value instanceof Collection) {
                return new NestedObjectIterator(
                        slice.from(pathIndex + 1),
                        slice.pathString(DISCOVERY_FIELD_PATH_SEPARATOR),
                        new SingleOrMultiObjectIterator<>(value),
                        strictly
                );
            } else if (slice.hasNextComp(pathIndex + 1)) {
                if (strictly) {
                    return new NestedObjectIterator(
                            slice.from(pathIndex + 1),
                            slice.pathString(DISCOVERY_FIELD_PATH_SEPARATOR),
                            new SingleOrMultiObjectIterator<>(value),
                            strictly
                    );
                } else {
                    if (value instanceof Map) {
                        properties = (Map<String,?>)value;
                    } else {
                        throw new IllegalArgumentException("Field path terminated at " + slice.pathStringUntil(pathIndex, DISCOVERY_FIELD_PATH_SEPARATOR));
                    }
                }
            } else if (value != null) {
                return new NestedObjectIterator(
                        slice.from(pathIndex + 1),
                        slice.pathString(DISCOVERY_FIELD_PATH_SEPARATOR),
                        new SingleOrMultiObjectIterator<>(value),
                        strictly
                );
            } else {
                return EMPTY_ITERATOR;
            }
        }
        return new NestedObjectIterator(
                slice,
                slice.pathString(DISCOVERY_FIELD_PATH_SEPARATOR),
                new SingleOrMultiObjectIterator<>(properties),
                strictly
        );
    }
}
