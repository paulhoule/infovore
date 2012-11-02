package com.ontology2.hydroxide.assembler;


import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.sparqlGrounder.O2Syntax;
import com.ontology2.sparqlGrounder.ParserSparqlGrounded;

public class Grounded {
	private static boolean registered=false;
	
	private static void register() {
		if(!registered) {
			ParserSparqlGrounded.register();
			registered=true;
		};
	}
	
	public static boolean ask(Dataset input,Query q) {
		register();
		QueryExecution qe=QueryExecutionFactory.create(q,input);
		return qe.execAsk();	
	}
	
	public static boolean ask(Model input,Query q) {
		register();
		QueryExecution qe=QueryExecutionFactory.create(q,input);
		return qe.execAsk();	
	}

	public static Query query(String queryText) {
		register();
		return QueryFactory.create(queryText,O2Syntax.syntaxGroundedSPARQL);
	}

	public static void construct(Dataset input, Query q, Model output) {
		register();
		QueryExecution qe=QueryExecutionFactory.create(q,input);
		qe.execConstruct(output);
	}
	
	public static Model construct(Model input, Query q) {
		register();
		QueryExecution qe=QueryExecutionFactory.create(q,input);
		return qe.execConstruct();
	}
}
