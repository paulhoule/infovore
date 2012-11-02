package com.ontology2.tripleFilter;

import java.io.Reader;

import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.sink.Sink;

public class TurtleParserWrapper {
	private final String filename;
	private final Sink<SituatedTriple> sink;

	public TurtleParserWrapper(String filename,Sink<SituatedTriple> sink) {
		this.filename=filename;
		this.sink=sink;
	}
	
	public void parse() throws Exception {
		Reader theReader=new FileOpener().createReader(filename);
		TurtleParser parser=new TurtleParser(theReader);
		parser.setEventHandler(new JenaSituatedTripleSink(filename,sink));
		parser.parse();
	}
}
