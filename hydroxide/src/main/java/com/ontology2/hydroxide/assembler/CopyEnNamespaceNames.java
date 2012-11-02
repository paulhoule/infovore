package com.ontology2.hydroxide.assembler;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class CopyEnNamespaceNames implements AssemblerStep {

	final Query copyQ=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"CONSTRUCT {?s ?p ?o} WHERE {" +
			"    GRAPH internal:knownAsGraph { ?s ?p ?o .} " +
			"    FILTER regex(STR(?o),'^http://rdf[.]basekb[.]com/ns/en')" +
			"}"
	);
	@Override
	public void applyRule(Resource subject,Dataset input, Model output) {
		Grounded.construct(input, copyQ, output);
	}

}
