package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.Codec;

public class CodecSink<T> extends EmptyReportSink<T> implements Sink<T> {

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
	public Model close() throws Exception {
		sink.close();
		return super.close();
	}

}
