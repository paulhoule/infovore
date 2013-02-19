package com.ontology2.hydroxide.primitiveTriples;

import com.ontology2.millipede.sink.Sink;

public class PrimitiveTripleReverser implements Sink<PrimitiveTriple> {
	private final Sink<PrimitiveTriple> innerSink;
	private final String from;
	private final String to;
	
	public PrimitiveTripleReverser(Sink<PrimitiveTriple> innerSink, String from, String to) {
		this.innerSink = innerSink;
		this.from = from;
		this.to = to;
	}
	
	@Override
	public void accept(PrimitiveTriple obj) throws Exception {
		if(from.equals(obj.predicate)) {
			innerSink.accept(new PrimitiveTriple(
					obj.object,
					to,
					obj.subject	
			));
		} else {
			innerSink.accept(obj);
		}	
	}

	@Override
	public void close() throws Exception {
		innerSink.close();
	}

}
