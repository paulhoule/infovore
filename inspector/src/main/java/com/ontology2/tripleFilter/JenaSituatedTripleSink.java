package com.ontology2.tripleFilter;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;
import com.ontology2.millipede.sink.Sink;

public class JenaSituatedTripleSink implements TurtleEventHandler {

	private final Sink<SituatedTriple> innerSink;
	private final String filename;
	private final boolean abortOnFail;
	
	public JenaSituatedTripleSink(String filename,Sink<SituatedTriple> innerSink) {
		this(filename,innerSink,true);
	}
	
	public JenaSituatedTripleSink(String filename,Sink<SituatedTriple> innerSink,boolean abortOnFail) {
		this.filename=filename;
		this.innerSink=innerSink;
		this.abortOnFail=abortOnFail;
	}


	@Override
	public void triple(int line, int col, Triple triple) {
		try {
			innerSink.accept(new SituatedTriple(filename, line, col, triple));
		} catch (Exception e) {
			e.printStackTrace();
			if(abortOnFail) {
				System.exit(-1);
			}
		}
	}
	
	@Override
	public void endFormula(int arg0, int arg1) {
	}

	@Override
	public void prefix(int arg0, int arg1, String arg2, String arg3) {
	}

	@Override
	public void startFormula(int arg0, int arg1) {
	}

}
