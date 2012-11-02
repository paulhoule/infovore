package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;

public abstract class NQuadsSubjectGroupModelFilter extends NQuadsGroupOnSubject {
	protected DatasetGraph model;

	@Override
	protected void openGroup() throws Exception {
		model=DatasetGraphFactory.createMem();
	}

	@Override
	protected void acceptItem(Quad q) throws Exception {
		model.add(q);
	}

}
