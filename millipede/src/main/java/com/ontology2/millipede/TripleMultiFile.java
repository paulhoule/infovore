package com.ontology2.millipede;

import org.apache.log4j.Logger;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;
import com.hp.hpl.jena.rdf.model.Model;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.JenaModelSink;
import com.ontology2.millipede.sink.JenaTripleSink;
import com.ontology2.millipede.sink.NTriplesSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.Source;

public class TripleMultiFile extends MultiFile<Triple> {
	static Logger logger = Logger.getLogger(Runner.class);
	
	public TripleMultiFile(String directory, String nameBase,
			String nameExtension, PartitionFunction<Triple> f) {
		super(directory, nameBase, nameExtension, f);
	}

	@Override
	public Sink<Triple> createSink(int binNumber) throws Exception {
		return new NTriplesSink(createOutputStream(binNumber));
	}


	@Override
	public void pushBin(int binNumber, Sink<Triple> destination)
			throws Exception {
		TurtleParser parser=new TurtleParser(createReader(binNumber));
		parser.setEventHandler(new JenaTripleSink(destination));
		parser.parse();
		destination.close();
	}
	
	public void fillJenaModel(Model m) throws Exception {
		JenaModelSink sink=new JenaModelSink(m);
		for(int i=0;i<getPartitionFunction().getPartitionCount();i++) {
			pushBin(i,sink);
		}
	}
	
	// Return raw strings
	
	public LineMultiFile<String> getLines() {
		return new LineMultiFile<String>(
				directory,
				nameBase,
				nameExtension,
				new DummyPartitionFunction(getPartitionCount()),
				new IdentityCodec());
	}
}
