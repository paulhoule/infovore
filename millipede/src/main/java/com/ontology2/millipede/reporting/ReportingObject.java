package com.ontology2.millipede.reporting;

import com.hp.hpl.jena.rdf.model.Model;

public interface ReportingObject {
	public Model close() throws Exception;
}
