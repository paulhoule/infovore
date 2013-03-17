package com.ontology2.hydroxide.files;

import com.ontology2.millipede.IdentityCodec;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.PartitionFunction;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.millipede.sink.SingleFileSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class DbpediaLiveConstellation extends InputFileConstellation {

	@Override
	public LineMultiFile<PrimitiveTriple> getPartitionedExpandedTriples() {
		PartitionFunction<PrimitiveTriple> partitionFunction = new PartitionPrimitiveTripleOnSubject(1024);
		return new LineMultiFile<PrimitiveTriple>(
				PartitionsAndFiles.resolveFilename("DBpediaLive/rawPartitioned") 
				,"triples"
				,PartitionsAndFiles.getCompressConfiguration("expandedRawFreebase",true) ? ".gz" : "", 
				partitionFunction,
				new PrimitiveTripleCodec());		
	}

	@Override
	public Sink<String> getRawRejected() throws Exception {
		String filename=PartitionsAndFiles.getWorkDirectory()+"/DBpediaLive/rawRejected.tsv";
		return new SingleFileSink<String>(new IdentityCodec(),filename);
	}

	@Override
	public SingleFileSource<String> getRawTriples() throws Exception {
		return SingleFileSource.createRaw(PartitionsAndFiles.getBaseDirectory()+"/dbpedia_2013_03_04.nt.bz2");
	}

	@Override
	public TripleMultiFile getDestination() throws Exception { 	
		return PartitionsAndFiles.createTripleMultiFile("/output/dbpediaLive/accepted",true);
	}
	
	@Override
	public LineMultiFile<PrimitiveTriple> getDestinationRejected() throws Exception { 	
		PartitionFunction<PrimitiveTriple> partitionFunction = new PartitionPrimitiveTripleOnSubject(1024);
		return new LineMultiFile<PrimitiveTriple>(
				PartitionsAndFiles.resolveFilename("/output/dbpediaLive/rejected") 
				,"triples"
				,PartitionsAndFiles.getCompressConfiguration("/output/dbpediaLive/accepted",true) ? ".gz" : "", 
				partitionFunction,
				new PrimitiveTripleCodec());
	}
}