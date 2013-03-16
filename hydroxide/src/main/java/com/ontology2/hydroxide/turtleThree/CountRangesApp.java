package com.ontology2.hydroxide.turtleThree;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.FileOpener;

public class CountRangesApp {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		String turtleTwo=PartitionsAndFiles.getTurtleTwoFile();
		Model t2Model=ModelFactory.createDefaultModel();
		t2Model.read(new FileOpener().createReader(turtleTwo),"http://rdf.basekb.com/","TURTLE");
		
		System.out.println(t2Model.size());
		
		showElementalTypes(t2Model);
		System.out.println();
		showCompositeTypes(t2Model);
	}

	private static void showElementalTypes(Model t2Model) {
		Query query=QueryFactory.create(
				"PREFIX bkbi: <http://rdf.basekb.com/internal/>" +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"" +
				"SELECT ?type (COUNT(?range) AS ?cnt) {" +
				"    ?property rdfs:range ?range ." +
				"    OPTIONAL { ?range bkbi:hasTypeTag ?type . }" + 
				"} GROUP BY ?type ORDER BY DESC(?cnt)"
		);
		
		QueryExecution qe=QueryExecutionFactory.create(query,t2Model);
		ResultSet results=qe.execSelect();
		while(results.hasNext()) {
			QuerySolution s=results.next();
			System.out.println(s);
		}
	}
	
	private static void showCompositeTypes(Model t2Model) {
		Query query=QueryFactory.create(
				"PREFIX bkbi: <http://rdf.basekb.com/internal/>" +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"" +
				"SELECT ?range (COUNT(?range) AS ?cnt) {" +
				"    ?property rdfs:range ?range ." +
				"    MINUS { ?range bkbi:hasTypeTag ?type . }" + 
				"} GROUP BY ?range ORDER BY DESC(?cnt) LIMIT 50"
		);
		
		QueryExecution qe=QueryExecutionFactory.create(query,t2Model);
		ResultSet results=qe.execSelect();
		while(results.hasNext()) {
			QuerySolution s=results.next();
			System.out.println(s);
		}
	}

}
