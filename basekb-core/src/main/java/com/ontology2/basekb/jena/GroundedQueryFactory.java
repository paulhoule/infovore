package com.ontology2.basekb.jena;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.Syntax;
import com.ontology2.basekb.IRIGrounder;
import com.ontology2.sparqlGrounder.O2Syntax;
import com.ontology2.sparqlGrounder.ParserSparqlGrounded;

//
// If you make more than one of these for a given Syntax,  the
// constructor will fail
//

public class GroundedQueryFactory implements AnyQueryFactory {
	private final IRIGrounder grounder;
	private final Syntax syntax;
	private final Map<String,String> prefixMapping;
	
	private boolean registered=false;

	
	public GroundedQueryFactory(IRIGrounder grounder,Map<String,String> prefixMapping,Syntax syntax) {
		this.grounder=grounder;
		this.prefixMapping=prefixMapping;
		this.syntax=syntax;
		register();
	}
	
	public GroundedQueryFactory(IRIGrounder grounder,Map<String,String> prefixMapping) {
		this(grounder,prefixMapping,O2Syntax.syntaxGroundedSPARQL);
	}
	
	public GroundedQueryFactory(IRIGrounder grounder) {
		this(grounder,new HashMap<String,String>());
	}
	
	private void register() {
		if(!registered) {
			ParserSparqlGrounded.register(grounder,prefixMapping,syntax);
			registered=true;
		};
	}
	
	//
	// we've got to convert to a string and back because there's no easy extension point
	// to make the groundedSPARQL query serializable,  which is necessary if we want to ask
	// the question to a remote endpoint
	//
	
	public Query create(String queryText) {
		Query groundedQuery=QueryFactory.create(queryText,O2Syntax.syntaxGroundedSPARQL);
		return QueryFactory.create(groundedQuery.toString(Syntax.syntaxSPARQL_11),Syntax.syntaxSPARQL_11);
	}

}
