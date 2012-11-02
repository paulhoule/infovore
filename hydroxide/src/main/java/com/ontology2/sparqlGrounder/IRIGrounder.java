package com.ontology2.sparqlGrounder;

import com.hp.hpl.jena.graph.Node;

//
// an IRIGround rewrites IRIs to allow people to 
//
public interface IRIGrounder {
	public Node ground(String iri) throws Exception;
}
