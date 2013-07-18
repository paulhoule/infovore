package com.ontology2.millipede.primitiveTriples;

import com.google.common.base.Function;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.sink.EmptyReportSink;
import com.ontology2.millipede.sink.Sink;

public class PrimitiveTriplePredicateRewriter implements Function<PrimitiveTriple,PrimitiveTriple> {
	private final String from;
	private final String to;
	
	public PrimitiveTriplePredicateRewriter(String from, String to) {
		this.from = from;
		this.to = to;
	}

	public PrimitiveTriple apply(PrimitiveTriple obj)  {
		if(from.equals(obj.predicate)) {
			return new PrimitiveTriple(
					obj.subject,
					to,
					obj.object	
			);
		} else {
			return obj;
		}	
	}
}
