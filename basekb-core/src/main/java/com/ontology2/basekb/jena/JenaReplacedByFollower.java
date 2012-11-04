package com.ontology2.basekb.jena;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.ontology2.basekb.BaseIRI;
import com.ontology2.basekb.IRIGrounder;

public class JenaReplacedByFollower implements IRIGrounder {
	
	private final RawConfiguration config;
	
	public JenaReplacedByFollower(RawConfiguration config) {
		this.config=config;
	}

	@Override
	public String lookup(String name) {
		// security preconditions
		
		if (config.getGraph().contains(">"))
			return null;
		
		if (name.contains(">"))
			return null;
		
		Query q=QueryFactory.create(
				"select ?destination {" +
				"    graph <"+config.getGraph()+"> {" +
				"       ?destination <"+BaseIRI.replaces+"> <"+name+"> ." +
				"    }" +
				"}"			
			);
		
		ResultSet r=config.getSparql().create(q).execSelect();
		
		if (!r.hasNext())
			return name;
		
		return r.next().get("destination").toString();
		
	}

}
