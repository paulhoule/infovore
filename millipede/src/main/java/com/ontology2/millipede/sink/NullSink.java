package com.ontology2.millipede.sink;

public class NullSink<T> implements Sink<T> {

	@Override
	public void accept(T obj) throws Exception {
	}

	@Override
	public void close() throws Exception {
	}

}
