package com.ontology2.millipede.sink;

import com.google.common.base.Function;
import com.hp.hpl.jena.rdf.model.Model;

public class FunctionSink<S,T> extends EmptyReportSink<S> {

    private final Function<S,T> innerFunction;
    private final Sink<T> innerSink;


    public FunctionSink(Function<S, T> innerFunction, Sink<T> innerSink) {
        this.innerFunction = innerFunction;
        this.innerSink = innerSink;
    }

    @Override
    public void accept(S obj) throws Exception {
        innerSink.accept(innerFunction.apply(obj));
    }

}
