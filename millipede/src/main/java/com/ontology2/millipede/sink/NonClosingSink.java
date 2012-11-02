package com.ontology2.millipede.sink;


public abstract class NonClosingSink<T> implements Sink<T> {

	@Override
	public void close() throws Exception {
	}

}
