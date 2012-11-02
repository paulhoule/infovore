package com.ontology2.millipede.sink;

import com.google.common.base.Predicate;

public class FilterSink<T> implements Sink<T> {
	
	private Sink<T> innerSink;
	private Predicate<T> predicate;

	public FilterSink(Sink<T> innerSink,Predicate<T> predicate) {
		this.innerSink=innerSink;
		this.predicate=predicate;
	}

	@Override
	public void accept(T obj) throws Exception {
		if (predicate.apply(obj)) {
			innerSink.accept(obj);
		}
		
	}

	@Override
	public void close() throws Exception {
		innerSink.close();
	}

}
