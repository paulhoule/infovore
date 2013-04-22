package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public abstract class EmptyReportSink<S> implements Sink<S> {


	@Override
	public Model close() throws Exception {
		return ModelFactory.createDefaultModel();
	}

}
