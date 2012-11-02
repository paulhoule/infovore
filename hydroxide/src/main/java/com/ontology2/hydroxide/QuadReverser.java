package com.ontology2.hydroxide;

import com.ontology2.millipede.sink.Sink;

public class QuadReverser implements Sink<FreebaseQuad> {
	private final Sink<FreebaseQuad> innerSink;
	private final String from;
	private final String to;
	
	public QuadReverser(Sink<FreebaseQuad> innerSink, String from, String to) {
		this.innerSink = innerSink;
		this.from = from;
		this.to = to;
	}

	@Override
	public void accept(FreebaseQuad obj) throws Exception {
		if(from.equals(obj.getProperty())) {
			innerSink.accept(new FreebaseQuad(
					obj.getDestination(),
					to,
					obj.getSubject(),
					obj.getValue()
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
