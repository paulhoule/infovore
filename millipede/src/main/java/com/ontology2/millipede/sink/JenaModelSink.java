package com.ontology2.millipede.sink;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;

public class JenaModelSink extends EmptyReportSink<Triple> implements Sink<Triple> {

	private final Model model;
	public JenaModelSink(Model model) {
		this.model=model;
	}
	
	@Override
	public void accept(Triple t) throws Exception {
		model.getGraph().add(t);		
	}

}
