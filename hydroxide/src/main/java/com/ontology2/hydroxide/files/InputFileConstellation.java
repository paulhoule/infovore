package com.ontology2.hydroxide.files;

import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public abstract class InputFileConstellation {
	abstract public LineMultiFile<PrimitiveTriple> getPartitionedExpandedTriples() throws Exception;
	abstract public Sink<String> getRawRejected() throws Exception;
	abstract public SingleFileSource<String> getRawTriples() throws Exception;
	abstract public TripleMultiFile getDestination() throws Exception;
	abstract public LineMultiFile<PrimitiveTriple> getDestinationRejected() throws Exception;
}
