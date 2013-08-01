package com.ontology2.millipede.source;

import java.util.Comparator;

public class OrderVerificationFilter<T> implements Source<T> {

    private final Source<T> innerSource;
    private final Comparator<T> comparator;

    private T lastElement;
    private boolean validElement;

    public OrderVerificationFilter(Source<T> innerSource,Comparator<T> comparator) {
        super();
        this.innerSource = innerSource;
        this.comparator = comparator;

        validElement=false;
        lastElement=null;
    }

    @Override
    public boolean hasMoreElements() {
        return innerSource.hasMoreElements();
    }

    @Override
    public T nextElement() throws Exception {
        T that=innerSource.nextElement();
        if (validElement) {
            if(comparator.compare(lastElement,that)>0)
                throw new Exception("Ordering is broken in source");
        } else {
            validElement=true;
        }

        lastElement=that;

        return that;
    }

}
