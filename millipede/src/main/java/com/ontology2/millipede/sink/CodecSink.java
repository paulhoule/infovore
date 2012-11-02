package com.ontology2.millipede.sink;

import com.ontology2.millipede.Codec;

public class CodecSink<T> implements Sink<T> {

	private final Codec codec;
	private final LineSink sink;

	public CodecSink(Codec codec,LineSink sink) {
		this.codec=codec;
		this.sink=sink;
	}
	
	@Override
	public void accept(T obj) throws Exception {
		sink.accept(codec.encode(obj));
	}

	@Override
	public void close() throws Exception {
		sink.close();
	}

}
