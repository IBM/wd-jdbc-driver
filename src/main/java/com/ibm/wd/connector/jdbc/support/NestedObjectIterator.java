package com.ibm.wd.connector.jdbc.support;

import static com.ibm.wd.connector.jdbc.model.WDConstants.DISCOVERY_FIELD_PATH_SEPARATOR_REGEX;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class NestedObjectIterator implements Iterator<Object> {

    private final PathCompSlice underneathSlice;
    private final String topFieldPath;
    private final Iterator<Object> source;
    private final boolean generateNestedStrictly;

    private NestedObjectIterator underneath;
    private NestedObjectIterator previousUnderneath;
    private Object fromSource;
    private int currentSeqNum = 0;

    public static NestedObjectIterator EMPTY_ITERATOR =
            new NestedObjectIterator(
                    new PathCompSlice("", DISCOVERY_FIELD_PATH_SEPARATOR_REGEX),
                    "",
                    Collections.emptyIterator(),
                    true) {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Object next() {
                    return null;
                }
            };

    public NestedObjectIterator(
            PathCompSlice underneathSlice,
            String topFieldPath,
            Iterator<Object> source,
            boolean generateNestedStrictly) {
        this.underneathSlice = underneathSlice;
        this.topFieldPath = topFieldPath;
        this.source = source;
        this.generateNestedStrictly = generateNestedStrictly;
        loadNextUnderneathIfRequired();
    }

    @Override
    public boolean hasNext() {
        return source.hasNext() || underneathHasNext();
    }

    private boolean underneathHasNext() {
        return underneath != null && underneath.hasNext();
    }

    private boolean allUnderneathHaveNext() {
        return !underneathSlice.hasNextComp()
                || underneathHasNext() && underneath.allUnderneathHaveNext();
    }

    @Override
    public Object next() {
        Object nextObject;
        if (underneathSlice.hasNextComp()) {
            nextObject = underneath.next();
            loadNextUnderneathIfRequired();
        } else {
            nextObject = source.next();
            fromSource = nextObject;
            currentSeqNum++;
        }
        return nextObject;
    }

    @SuppressWarnings("unchecked")
    private void loadNextUnderneathIfRequired() {
        if (previousUnderneath != null) {
            previousUnderneath = null;
        }
        if (underneathSlice.hasNextComp()) {
            NestedObjectIterator tempPrev = underneath;
            while (!allUnderneathHaveNext() && source.hasNext()) {
                logCurrentPosition("in loop of load next");
                fromSource = source.next();
                underneath = WDDocValueExtractor.generateNestedObjectIterator(
                        underneathSlice, (Map<String, ?>) fromSource, generateNestedStrictly);
                currentSeqNum++;
            }
            logCurrentPosition("after loop of load next");
            if (tempPrev != underneath) {
                previousUnderneath = tempPrev;
            }
        }
    }

    private NestedObjectIterator getIteratorHoldingCurrentSequence() {
        return isPreviousHoldingCurrentSequence() ? previousUnderneath : underneath;
    }

    private boolean isPreviousHoldingCurrentSequence() {
        return previousUnderneath != null;
    }

    public Pair<String, Object> getCurrentObjectHavingPath(String[] pathComps) {
        if (underneathSlice.relatively(pathComps) == PathCompSlice.MatchState.MAY_BRANCH_NEXT) {
            return getIteratorHoldingCurrentSequence().getCurrentObjectHavingPath(pathComps);
        }
        return new ImmutablePair<>(topFieldPath, fromSource);
    }

    public Pair<String, Integer> getCurrentSequenceNumberHavingPath(String[] pathComps) {
        if (underneathSlice.relatively(pathComps) == PathCompSlice.MatchState.MAY_BRANCH_NEXT) {
            return getIteratorHoldingCurrentSequence()
                    .getCurrentSequenceNumberHavingPath(pathComps);
        }
        return new ImmutablePair<>(
                topFieldPath,
                isPreviousHoldingCurrentSequence() ? currentSeqNum - 1 : currentSeqNum);
    }

    private void logCurrentPosition(String method) {
        //        System.err.println(method + ": " + topFieldPath + "[" + currentSeqNum + "]" +
        //                ", under have next: " + underneathHasNext() +
        //                ", all underneath have next: " + allUnderneathHaveNext() +
        //                ", I have next: " + source.hasNext()
        //        );
    }
}
