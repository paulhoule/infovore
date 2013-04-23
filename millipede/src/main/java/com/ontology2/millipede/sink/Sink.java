package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.reporting.ReportingObject;

public interface Sink<S> extends ReportingObject {
	public void accept(S obj) throws Exception;
}
