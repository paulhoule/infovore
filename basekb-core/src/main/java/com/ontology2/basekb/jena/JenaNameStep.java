package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.ontology2.basekb.BaseIRI;
import com.ontology2.basekb.NameResolutionStep;

public class JenaNameStep implements NameResolutionStep {
	
	private final RawConfiguration config;


	public JenaNameStep(RawConfiguration config) {
		this.config=config;
	}
	
	
	@Override
	public String lookup(String namespace, String name) {
		
		// security preconditions
		
		if (config.getGraph().contains(">"))
			return null;
		
		if (namespace.contains(">"))
			return null;
					
		if (name.contains("\""))
			return null;
		
		Query q=QueryFactory.create(
			"select ?destination {" +
			"    graph <"+config.getGraph()+"> {" +
			"       ?destination <"+namespace+"> \""+name+"\" ." +
			"    }" +
			"}"			
		);
		
		ResultSet qe=config.getSparql().create(q).execSelect();
		if (!qe.hasNext()) {
			return null;
		}
		
		return qe.next().get("destination").toString();
	}

}
