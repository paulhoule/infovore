package com.ontology2.hydroxide.files;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ontology2.millipede.IdentityCodec;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.PartitionFunction;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.sink.SingleFileSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class StandardFileConstellation extends InputFileConstellation {
	private final String internalName;
	private static Log logger = LogFactory.getLog(StandardFileConstellation.class);

	public StandardFileConstellation(String internalName) {
		this.internalName = internalName;
	}

	@Override
	public LineMultiFile<PrimitiveTriple> getPartitionedExpandedTriples() {
		String name=internalName+"/rawPartitioned";
		return createPrimitiveTripleMultifile(name);		
	}

	private LineMultiFile<PrimitiveTriple> createPrimitiveTripleMultifile(String name) {
		PartitionFunction<PrimitiveTriple> partitionFunction = new PartitionPrimitiveTripleOnSubject(1024);
		return new LineMultiFile<PrimitiveTriple>(
				PartitionsAndFiles.resolveFilename(name) 
				,"triples"
				,PartitionsAndFiles.getCompressConfiguration(name,true) ? ".gz" : "", 
				partitionFunction,
				new PrimitiveTripleCodec());
	}

	@Override
	public Sink<String> getRawRejected() throws Exception {
		String filename=PartitionsAndFiles.getInstanceDirectory()+"/"+internalName+"/rawRejected.tsv";
		return new SingleFileSink<String>(new IdentityCodec(),filename);
	}

	@Override
	public TripleMultiFile getDestination() throws Exception { 	
		return PartitionsAndFiles.createTripleMultiFile(internalName+"/accepted",true);
	}
	
	@Override
	public LineMultiFile<PrimitiveTriple> getDestinationRejected() throws Exception {
		String name=internalName+"/rejected";
		return createPrimitiveTripleMultifile(name);
	}

}
