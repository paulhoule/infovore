package com.ontology2.hydroxide.partitionNTriplesApp;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples;
import com.ontology2.hydroxide.files.DbpediaLiveConstellation;
import com.ontology2.hydroxide.files.InputFileConstellation;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.files.ReadNTriples;
import com.ontology2.hydroxide.files.StandardFileConstellation;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriplePredicateRewriter;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleReverser;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.sink.FilterSink;
import com.ontology2.millipede.sink.ProgressReportingSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class PartitionNTriplesApp extends CommandLineApplication {

	@Override
	protected void _run(String[] args) throws Exception {
		if(args.length<2) {
			die("partionNTriples [input filename] [project name]");
		}
		
		String inputFilename=args[0];
		String projectName=args[1];

		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);
		SingleFileSource<String> input=SingleFileSource.createRaw(inputFilename);

		InputFileConstellation files=new StandardFileConstellation(projectName);
		LineMultiFile<PrimitiveTriple> output=files.getPartitionedExpandedTriples();
		Sink<String> rejects=files.getRawRejected();

		if(output.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Partitioner<PrimitiveTriple> p=new Partitioner<PrimitiveTriple>(output);
		ReadNTriples cleaner=new ReadNTriples(p,rejects);
		ProgressReportingSink prs=new ProgressReportingSink(cleaner);
		Plumbing.flow(input,prs);	
	}


}
