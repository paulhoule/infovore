package com.ontology2.hydroxide.assembler;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class CopyTurtleThree implements AssemblerStep {

	final Query copyT3Q=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"CONSTRUCT { ?s ?p ?o } WHERE { " +
			"    GRAPH internal:turtle3Graph { ?s ?p ?o .}   " +
			"}");
	
	@Override
	public void applyRule(Resource subject,Dataset input, Model output) {
		Grounded.construct(input, copyT3Q, output);
	}

}
