package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.sink.Sink;

public class QuadToTriple2 implements Sink<FreebaseQuad> {
	Sink<Triple> innerSink;
	
	public QuadToTriple2(Sink<Triple> innerSink) {
		this.innerSink=innerSink;
	}

	@Override
	public void accept(FreebaseQuad obj) throws Exception {

	}

	@Override
	public void close() throws Exception {
		innerSink.close();
	}
}
