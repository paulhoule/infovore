package com.ontology2.millipede.sink;

import com.hp.hpl.jena.rdf.model.Model;


public class ProgressReportingSink<S> implements Sink<S> {

	private final Sink<S> innerSink;
	private int count=0;

	public ProgressReportingSink(Sink<S> innerSink) {
		this.innerSink = innerSink;
	}

	@Override
	public void accept(S obj) throws Exception {
		innerSink.accept(obj);
		count++;
		if (count % 100000==0) {
			System.out.format("%,d\n",count);
		}
	}

	@Override
	public Model close() throws Exception {
		return innerSink.close();
	}

}
