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

public class DbpediaLiveConstellation extends StandardFileConstellation {
	public DbpediaLiveConstellation() {
		super("DBpediaLive");
	}
}