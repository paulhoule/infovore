package com.ontology2.millipede.primitiveTriples;

import com.google.common.base.Function;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.sink.EmptyReportSink;
import com.ontology2.millipede.sink.Sink;

public class PrimitiveTripleReverser implements Function<PrimitiveTriple,PrimitiveTriple> {
	
	final private String from;
	final private String to;

	public PrimitiveTripleReverser(String from, String to) {
		this.from = from;
		this.to = to;
	}
	
	@Override
	public PrimitiveTriple apply(PrimitiveTriple obj) {
		if(from.equals(obj.predicate)) {
			return new PrimitiveTriple(
					obj.object,
					to,
					obj.subject	
			);
		} else {
			return obj;
		}	
	}


}
