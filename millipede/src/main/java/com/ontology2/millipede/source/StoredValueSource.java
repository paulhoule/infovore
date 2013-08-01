package com.ontology2.millipede.source;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;

public class StoredValueSource<T> implements Source<T> {

    final UnmodifiableIterator<T> iterator;
    public StoredValueSource(Collection<T> collection) {
        ImmutableList<T> list=ImmutableList.copyOf(collection);
        iterator=list.iterator();
    }

    @Override
    public boolean hasMoreElements() {
        return iterator.hasNext();
    }

    @Override
    public T nextElement() throws Exception {
        return iterator.next();
    }

}
