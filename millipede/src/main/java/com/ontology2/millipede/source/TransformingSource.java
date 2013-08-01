package com.ontology2.millipede.source;

import com.google.common.base.Function;

public class TransformingSource<S,T> implements Source<T> {

    public final Source<S> innerSource;
    public final Function<S,T> transformingFunction;

    public TransformingSource(Source<S> innerSource,Function<S,T> transformingFunction) {
        this.innerSource = innerSource;
        this.transformingFunction=transformingFunction;
    }

    @Override
    public boolean hasMoreElements() {
        return innerSource.hasMoreElements();
    }

    @Override
    public T nextElement() throws Exception {
        return transformingFunction.apply(innerSource.nextElement());
    }

}
