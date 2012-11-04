package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;

public class SparqlProtocol implements AnyQueryExecutionFactory {

	final String service;
	
	public SparqlProtocol(String service) {
		this.service=service;
	}
	
	@Override
	public QueryExecution create(Query query) {
		return QueryExecutionFactory.sparqlService(service, query);
	}

}
