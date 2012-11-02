package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

abstract public class SubjectGroupModelFilter extends GroupOnSubject {
	
	protected Model model;

	@Override
	protected void openGroup() throws Exception {
		model=ModelFactory.createDefaultModel();
	}

	@Override
	protected void acceptItem(Triple obj) throws Exception {
		model.add(model.asStatement(obj));
	}

}
