package com.ontology2.millipede.sink;

public interface Sink<S> {
	public void accept(S obj) throws Exception;
	public void close() throws Exception;
}
