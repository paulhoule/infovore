package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.sink.Sink;

public abstract class StatefulTripleFilter extends SubjectGroupModelFilter {
	
	protected Sink<Triple> innerSink;
	
	public StatefulTripleFilter(Sink<Triple> innerSink) {
		this.innerSink=innerSink;
	}

	@Override
	public void close$() throws Exception {
		innerSink.close();
	}
}
