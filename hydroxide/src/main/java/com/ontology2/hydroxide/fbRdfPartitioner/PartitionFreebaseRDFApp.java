package com.ontology2.hydroxide.fbRdfPartitioner;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.ontology2.hydroxide.ExpandFreebaseRdfToNTriples;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionOnSubject;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
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

public class PartitionFreebaseRDFApp extends CommandLineApplication  {
	
	@Override
	protected void _run(String[] args) throws Exception {
		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);
		
		LineMultiFile<PrimitiveTriple> output=PartitionsAndFiles.getPartitionedExpandedTriples();
		Sink<String> rejects=PartitionsAndFiles.getRawFreebaseRejected();
		SingleFileSource<String> input=PartitionsAndFiles.getRawTriples();
		if(output.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
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
		
		filter=new FilterSink<PrimitiveTriple>(filter, tripleFilter);
		ExpandFreebaseRdfToNTriples expand=new ExpandFreebaseRdfToNTriples(filter,rejects);

		ProgressReportingSink prs=new ProgressReportingSink(expand);
		Plumbing.flow(input,prs);
	}


}
