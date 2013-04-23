package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.millipede.reporting.ReportingVocabulary;

public class ReportingObject {
	private final ReportingCloseImplementation impl;
	
	public ReportingObject() {
		impl = new ReportingCloseImplementation(this);
		summary=impl.summary;
		v = new ReportingVocabulary(impl.summary);
		me=impl.me;
	}

	protected final Model summary;
	protected final ReportingVocabulary v;
	protected final Resource me;

	public Model close() throws Exception {
		return impl.close();
	}

}
