package com.ontology2.basekb.jena;

import com.ontology2.basekb.IRIGrounder;

public interface GroundedSparqlConfiguration {
	public RawConfiguration getRawConfiguration();
	public IRIGrounder getIRIGrounder();
	public AnyQueryFactory getGroundedQueryFactory();
}
