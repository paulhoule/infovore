package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;

public interface AnyQueryFactory {
	public Query create(String queryText);
}
