package com.ontology2.hydroxide.partitionNTriplesApp;

import java.util.UUID;

import org.apache.jena.iri.IRI;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.hp.hpl.jena.rdf.model.Property;
import com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples;
import com.ontology2.hydroxide.files.DbpediaLiveConstellation;
import com.ontology2.hydroxide.files.InputFileConstellation;
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
import com.ontology2.millipede.uuid.ResourceSupplier;
import com.ontology2.millipede.uuid.UUIDSupplier;

public class PartitionNTriplesApp extends InfovoreApplication {
	
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

		dontOverwrite(output);
		
		Partitioner<PrimitiveTriple> p=new Partitioner<PrimitiveTriple>(output);
		ReadNTriples cleaner=new ReadNTriples(p,rejects);
		ProgressReportingSink prs=new ProgressReportingSink(cleaner);
		long inputCount=Plumbing.flow(input,prs);

		identifySelf();
		identifyInputFile(input);
		
		me.addLiteral(v.grosslyMalformedFacts(),cleaner.getCountInvalid());
		me.addLiteral(v.commentCount(),cleaner.getCountComments());
		me.addLiteral(v.inputTripleCount(),inputCount);
		me.addLiteral(v.outputTripleCount(),p.getFactCount());
		
		writeSummaryFile(output);
	}


}
