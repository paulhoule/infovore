package com.ontology2.hydroxide.primitiveTriples;

import com.ontology2.millipede.sink.Sink;

public class PrimitiveTripleTap implements Sink<PrimitiveTriple> {
	Sink<PrimitiveTriple> innerSink;

	public PrimitiveTripleTap(Sink<PrimitiveTriple> innerSink) {
		this.innerSink=innerSink;
	}
	
	@Override
	public void accept(PrimitiveTriple obj) throws Exception {
		System.out.println(obj);
		innerSink.accept(obj);
	}

	@Override
	public void close() throws Exception {
		innerSink.close();
	}

}
