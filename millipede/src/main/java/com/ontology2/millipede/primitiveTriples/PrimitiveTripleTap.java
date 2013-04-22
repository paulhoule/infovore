package com.ontology2.millipede.primitiveTriples;

import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.sink.EmptyReportSink;
import com.ontology2.millipede.sink.Sink;

public class PrimitiveTripleTap extends EmptyReportSink<PrimitiveTriple> {
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
	public Model close() throws Exception {
		innerSink.close();
		return super.close();
	}

}
