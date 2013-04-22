package com.ontology2.millipede.sink;

import com.google.common.base.Predicate;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class FilterSink<T> implements Sink<T> {
	
	private Sink<T> innerSink;
	private Predicate<T> predicate;
	
	long acceptedCount=0;
	long rejectedCount=0;

	public FilterSink(Sink<T> innerSink,Predicate<T> predicate) {
		this.innerSink=innerSink;
		this.predicate=predicate;
	}

	@Override
	public void accept(T obj) throws Exception {
		if (predicate.apply(obj)) {
			innerSink.accept(obj);
			acceptedCount++;
		} else {
			rejectedCount++;
		}
	}

	@Override
	public Model close() throws Exception {
		innerSink.close();
		return ModelFactory.createDefaultModel();
	}

	public long getAcceptedCount() {
		return acceptedCount;
	}

	public long getRejectedCount() {
		return rejectedCount;
	}
	
	

}
