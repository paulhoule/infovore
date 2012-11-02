package com.ontology2.sparqlGrounder;

import java.io.InputStream;
import java.io.Reader;

import com.google.inject.Inject;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11TokenManager;

public class SparqlParserGrounded extends SPARQLParser11 {

	IRIGrounder grounder=TurtleZeroGrounder.create();
	
	public SparqlParserGrounded(InputStream stream, java.lang.String encoding) {
		super(stream, encoding);
	}

	public SparqlParserGrounded(InputStream stream) {
		super(stream);
	}

	public SparqlParserGrounded(Reader stream) {
		super(stream);
	}

	public SparqlParserGrounded(SPARQLParser11TokenManager tm) {
		super(tm);
	}

	@Override
	protected Node createNode(String iri) {
		try {
			return grounder.ground(iri);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

}
