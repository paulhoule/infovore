package com.ontology2.hydroxide.turtleThree;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.ontology2.hydroxide.turtleZero.TurtleZero;

public class CreateRulebox {

	public static Model createLangModel(TurtleZero t0) {
		Model theModel=ModelFactory.createDefaultModel();
		return theModel;
	}

}
