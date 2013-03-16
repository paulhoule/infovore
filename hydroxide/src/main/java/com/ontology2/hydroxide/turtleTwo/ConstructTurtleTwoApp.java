package com.ontology2.hydroxide.turtleTwo;

import java.io.PrintWriter;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.rdf.JenaUtil;

public class ConstructTurtleTwoApp {
	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		TripleMultiFile in= PartitionsAndFiles.getTurtleTwoFacts();
		Model inModel=ModelFactory.createDefaultModel();
		in.fillJenaModel(inModel);
		
		Model outModel=ModelFactory.createDefaultModel();
		JenaUtil.appendConstruct(outModel,
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"" +
				"CONSTRUCT WHERE {" +
				"   ?s rdfs:range ?o ." +
				"}",
				inModel);

		JenaUtil.appendConstruct(outModel,
				"PREFIX bkbi: <http://rdf.basekb.com/internal/>" +
				"" +
				"CONSTRUCT WHERE {" +
				"   ?s bkbi:hasTypeTag ?o ." +
				"}",
				inModel);
		
		JenaUtil.appendConstruct(outModel,
				"PREFIX bkbi: <http://rdf.basekb.com/internal/>" +
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
				"" +
				"CONSTRUCT {" +
				"   ?s bkbi:hasLangTag ?o ." +
				"} WHERE {" +
				"   SELECT ?s (MIN(lcase(?tag)) as ?o) WHERE {" +
				"       ?s bkbi:hasLangTag ?tag " +
				"   } GROUP BY ?s" +
				"}",
				inModel);
		
		PrintWriter w=new FileOpener().createWriter(PartitionsAndFiles.getTurtleTwoFile());
		outModel.write(w,"TURTLE");
	}
}
