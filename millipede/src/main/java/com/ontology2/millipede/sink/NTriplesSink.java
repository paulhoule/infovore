package com.ontology2.millipede.sink;

import java.io.OutputStream;
import java.io.Writer;

import org.openjena.riot.out.SinkTripleOutput;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;

public class NTriplesSink extends EmptyReportSink<Triple> {

	private final SinkTripleOutput innerSink;
	private final OutputStream stream;
	
	public NTriplesSink(OutputStream stream) {
		this.stream=stream;
		this.innerSink=new SinkTripleOutput(stream);
	}

	@Override
	public void accept(Triple obj) throws Exception {
		innerSink.send(obj);
	}

	@Override
	public Model close() throws Exception {
		innerSink.close();
		stream.close();
		return super.close();
	}
}
