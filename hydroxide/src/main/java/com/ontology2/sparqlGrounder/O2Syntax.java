package com.ontology2.sparqlGrounder;

import com.hp.hpl.jena.query.Syntax;

public class O2Syntax extends Syntax {

	public static final Syntax syntaxGroundedSPARQL=new O2Syntax("http://rdf.basekb.com/jena/GroundedSparql");
	
	protected O2Syntax(String s) {
		super(s);
		// TODO Auto-generated constructor stub
	}

	protected O2Syntax(Syntax s) {
		super(s);
		// TODO Auto-generated constructor stub
	}

}
