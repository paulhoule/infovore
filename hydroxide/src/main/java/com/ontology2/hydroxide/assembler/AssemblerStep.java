package com.ontology2.hydroxide.assembler;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public interface AssemblerStep {
	public void applyRule(Resource subject, Dataset input,Model output) throws Exception;
}
