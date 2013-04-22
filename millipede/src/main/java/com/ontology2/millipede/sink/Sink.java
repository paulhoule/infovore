package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;

public interface Sink<S> {
	public void accept(S obj) throws Exception;
	public Model close() throws Exception;
}
