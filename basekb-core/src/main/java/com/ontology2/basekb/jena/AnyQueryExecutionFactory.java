package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;

public interface AnyQueryExecutionFactory {
	public QueryExecution create(Query q);
}
