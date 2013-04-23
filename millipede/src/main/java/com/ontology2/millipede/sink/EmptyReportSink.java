package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.millipede.reporting.ReportingVocabulary;

public abstract class EmptyReportSink<S> extends ReportingObject implements Sink<S> {

}
