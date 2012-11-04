package com.ontology2.basekb.util;

import static com.ontology2.basekb.jena.StatelessJenaFunctions.fromFb;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class LiveTurtleApp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length!=1) {
			System.out.println("Must give exactly one argument,  a Freebase key");
			return;
		}
		
		String key=args[0];
		String url=fromFb(key).getURI();
		
		Model m=ModelFactory.createDefaultModel();
		m.read(url);
		m.write(System.out,"TURTLE");
	}

}
