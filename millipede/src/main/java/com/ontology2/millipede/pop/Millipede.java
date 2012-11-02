package com.ontology2.millipede.pop;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.sink.Sink;

public interface Millipede<InT> {
	public Sink<InT> createSegment(int segmentNumber) throws Exception;
}
