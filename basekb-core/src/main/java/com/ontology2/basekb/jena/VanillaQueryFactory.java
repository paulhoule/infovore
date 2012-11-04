package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class VanillaQueryFactory implements AnyQueryFactory {

	@Override
	public Query create(String queryText) {
		return QueryFactory.create(queryText);
	}

}
