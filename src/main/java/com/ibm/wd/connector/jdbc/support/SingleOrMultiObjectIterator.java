package com.ibm.wd.connector.jdbc.support;

import java.util.Iterator;

public class SingleOrMultiObjectIterator<T> implements Iterator<T> {

    private T singleRef;
    private Iterator<T> multipleRef;

    @SuppressWarnings({"unchecked"})
    public SingleOrMultiObjectIterator(Object ref) {
        if (ref instanceof Iterable<?>) {
            multipleRef = ((Iterable<T>) ref).iterator();
        } else {
            singleRef = (T)ref;
        }
    }

    @Override
    public boolean hasNext() {
        if (multipleRef != null) {
            return multipleRef.hasNext();
        } else {
            return singleRef != null;
        }
    }

    @Override
    public T next() {
        if (multipleRef != null) {
            return multipleRef.next();
        } else {
            T ref = singleRef;
            singleRef = null;
            return ref;
        }
    }
}
