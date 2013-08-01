package com.ontology2.millipede.sink;

public class NullSink<T> extends EmptyReportSink<T> {

    @Override
    public void accept(T obj) throws Exception {
    }

}
