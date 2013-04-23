package com.ontology2.millipede.reporting;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class ReportingCloseImplementation {
	private final Object client;
	
	public ReportingCloseImplementation(Object client) {
		this.client = client;
	}

	public final Model summary = ModelFactory.createDefaultModel();
	public final ReportingVocabulary v = new ReportingVocabulary(summary);
	public final Resource me = v.something();

	public Model close() throws Exception {
		return summary;
	}
}
