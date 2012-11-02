package com.ontology2.hydroxide.assembler;

import com.google.common.collect.Iterators;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class CopyAllNamesForSchemaObjects implements AssemblerStep {

	final Query nameQ=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"ASK { " +
			"    GRAPH internal:turtle3Graph {" +
			"        { ?s a basekb:type.property . }  " +
			"        UNION { ?s a basekb:type.type } " +
			"        UNION { ?s a basekb:type.domain } " +
			"        UNION { ?s a basekb:type.namespace } " +
			"        MINUS { ?s a basekb:book.isbn } " +
			"    } " +
			"}");

	final Query copyNames=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"CONSTRUCT { ?s ?p ?o } WHERE { " +
			"    GRAPH internal:knownAsGraph { ?s ?p ?o .}   " +
			"}");
	
	
	@Override
	public void applyRule(Resource subject,Dataset input, Model output) {
		Model m=input.getNamedModel("http://rdf.basekb.com/internal/turtle3Graph");

		if(Grounded.ask(input,nameQ)) {
			Grounded.construct(input,copyNames,output);
		}
	}
}
