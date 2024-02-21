package com.ibm.wd.connector.jdbc.support;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class PathCompSlice {

    public enum MatchState {
        UNDERNEATH,
        OUTSIDE,
        MAY_BRANCH_NEXT,
        ABOVE,
    }

    private final String[] pathComps;
    private final int fromPathIndex;

    public PathCompSlice(String fieldPath, String pathSeparator) {
        if (fieldPath == null || fieldPath.isEmpty()) {
            pathComps = new String[]{};
            fromPathIndex = -1;
        } else {
            pathComps = fieldPath.split(pathSeparator);
            fromPathIndex = 0;
        }
    }

    private PathCompSlice(String[] pathComps, int fromPathIndex) {
        this.pathComps = pathComps;
        this.fromPathIndex = fromPathIndex;
    }

    public boolean hasNextComp(int from) {
        return fromPathIndex <= from && from < pathComps.length;
    }

    public boolean hasNextComp() {
        return hasNextComp(fromPathIndex);
    }

    public boolean mayHaveNextComp(int from) {
        return hasNextComp(from) || from == pathComps.length;
    }

    public MatchState relatively(String[] comps) {
        int compIndex = 0;
        for (; compIndex < fromPathIndex; compIndex++) {
            if (comps.length <= compIndex) {
                return MatchState.UNDERNEATH;
            }
            if (!comps[compIndex].equals(pathComps[compIndex])) {
                return MatchState.OUTSIDE;
            }
        }
        if (compIndex < pathComps.length) {
            if (comps[compIndex].equals(pathComps[compIndex])) {
                return MatchState.MAY_BRANCH_NEXT;
            } else {
                return MatchState.OUTSIDE;
            }
        } else {
            return MatchState.ABOVE;
        }
    }

    public String pathStringUntil(int until, String separator) {
        if (hasNextComp(until)) {
            return String.join(separator, Arrays.asList(pathComps).subList(0, until+1));
        } else {
            return "";
        }
    }

    public String pathString(String separator) {
        return pathStringUntil(fromPathIndex, separator);
    }

    public PathCompSlice from(int from) {
        if (mayHaveNextComp(from)) {
            return new PathCompSlice(pathComps, from);
        } else {
            throw new IllegalArgumentException("path comps slice cannot be split from " + from);
        }
    }

    public Iterator<Pair<String, Integer>> iterator() {
        if (hasNextComp()) {
            return new Iterator<Pair<String, Integer>>() {
                private int currentIndex = fromPathIndex;

                @Override
                public boolean hasNext() {
                    return hasNextComp(currentIndex);
                }

                @Override
                public Pair<String, Integer> next() {
                    String pathComp = pathComps[currentIndex];
                    return new ImmutablePair<>(pathComp, currentIndex++);
                }
            };
        } else {
            return Collections.emptyIterator();
        }
    }

}
