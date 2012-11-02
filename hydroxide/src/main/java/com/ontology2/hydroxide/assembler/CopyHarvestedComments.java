package com.ontology2.hydroxide.assembler;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.hydroxide.turtleZero.ConstructTurtleZeroApp;

public class CopyHarvestedComments implements AssemblerStep {

	static Logger logger=Logger.getLogger(CopyHarvestedComments.class);
	
	final Query copyComments=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"CONSTRUCT { ?s ?p ?o } WHERE { " +
			"    GRAPH internal:harvestedCommentGraph {" +
			"       ?s ?p ?o ." +
			"       filter (strlen(?o)<20000)" +
			"    }   " +
			"}");
	
	final Query oversizedComments=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"ASK { " +
			"    GRAPH internal:harvestedCommentGraph {" +
			"       ?s ?p ?o ." +
			"       filter (strlen(?o)>=20000)" +
			"    }   " +
			"}");
	
	@Override
	public void applyRule(Resource subject, Dataset input, Model output)
			throws Exception {
		Grounded.construct(input, copyComments, output);
		boolean oversized=Grounded.ask(input, oversizedComments);
		if (oversized) {
			logger.warn("Oversized comment for "+subject);
		}
	}

}
