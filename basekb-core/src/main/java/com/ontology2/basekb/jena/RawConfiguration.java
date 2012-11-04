package com.ontology2.basekb.jena;

import com.ontology2.basekb.BaseIRI;


public class RawConfiguration {
	private final AnyQueryExecutionFactory sparql;
	private final String graph;
	
	public RawConfiguration(AnyQueryExecutionFactory sparql, String graph) {
		this.sparql = sparql;
		this.graph = graph;
	}
	
	public RawConfiguration(AnyQueryExecutionFactory sparql) {
		this(sparql,BaseIRI.bkGraph);
	}

	public String getGraph() {
		return graph;
	}
	
	public AnyQueryExecutionFactory getSparql() {
		return sparql;
	}

}
