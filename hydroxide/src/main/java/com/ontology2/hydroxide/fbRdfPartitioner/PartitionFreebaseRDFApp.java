package com.ontology2.hydroxide.fbRdfPartitioner;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionOnSubject;
import com.ontology2.hydroxide.files.InputFileConstellation;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.files.StandardFileConstellation;
import com.ontology2.hydroxide.partitionNTriplesApp.InfovoreApplication;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.Partitioner;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriplePredicateRewriter;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleReverser;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleTap;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.sink.FilterSink;
import com.ontology2.millipede.sink.ProgressReportingSink;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.SingleFileSource;

public class PartitionFreebaseRDFApp extends InfovoreApplication  {
	
	@Override
	protected void _run(String[] args) throws Exception {
		if(args.length<1) {
			die("partitionFreebaseRDF [input filename] [output filename?]");
		}
		
		String inputFilename=args[0];
		String outputProject=args.length>1 ? args[1] : "baseKBLime"; 
		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);
		SingleFileSource<String> input=SingleFileSource.createRaw(inputFilename);
		
		InputFileConstellation files=new StandardFileConstellation(outputProject);
		LineMultiFile<PrimitiveTriple> output=files.getPartitionedExpandedTriples();
		Sink<String> rejects=files.getRawRejected();

		dontOverwrite(output);
		
		Predicate<PrimitiveTriple> tripleFilter=Predicates.not(Predicates.or(
				PrimitiveTriple.hasPredicate("<http://rdf.freebase.com/ns/type.type.instance>"),
				PrimitiveTriple.hasPredicate("<http://rdf.freebase.com/ns/type.type.expected_by>"),
				Predicates.and(
						PrimitiveTriple.hasPredicate("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
						PrimitiveTriple.objectMatchesPrefix("<http://rdf.freebase.com")
				)
		));
		
		Partitioner<PrimitiveTriple> p=new Partitioner<PrimitiveTriple>(output);
				
		Sink<PrimitiveTriple> filter=new PrimitiveTripleReverser(p
				,"http://rdf.freebase.com/ns/type.permission.controls"
				,"http://rdf.freebase.com/ns/m.0j2r9sk");
		filter=new PrimitiveTripleReverser(filter
				,"http://rdf.freebase.com/ns/dataworld.gardening_hint.replaced_by"
				,"http://rdf.freebase.com/ns/m.0j2r8t8");
		filter=new PrimitiveTriplePredicateRewriter(filter,
				"<http://rdf.freebase.com/ns/type.object.type>",
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		
		FilterSink f2=new FilterSink<PrimitiveTriple>(filter, tripleFilter);
		ExpandFreebaseRdfToNTriples expand=new ExpandFreebaseRdfToNTriples(f2,rejects);

		ProgressReportingSink prs=new ProgressReportingSink(expand);
		long inputCount=Plumbing.flow(input,prs);
		
		initializeModel();
		identifyInputFile(input);
		
		me.addLiteral(v.inputTripleCount(),inputCount);
		me.addLiteral(v.outputTripleCount(),p.getFactCount());
		me.addLiteral(v.grosslyMalformedFacts(),expand.getGrosslyMalformedCount());
		me.addLiteral(v.prefixDeclCount(), expand.getPrefixDeclCount());
		me.addLiteral(v.rawAcceptedCount(), expand.getRawAcceptedCount());
		me.addLiteral(v.refusedCount(), f2.getRejectedCount());
		
		writeSummaryFile(output);
	}
}
