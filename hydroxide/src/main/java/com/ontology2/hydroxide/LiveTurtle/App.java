package com.ontology2.hydroxide.LiveTurtle;

import java.io.Console;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class App {

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
