package com.ontology2.hydroxide.parallelSuperEyeball;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.PartitionPrimitiveTripleOnSubject;
import com.ontology2.hydroxide.PartitionsAndFiles;
import com.ontology2.hydroxide.PrimitiveTriple;
import com.ontology2.hydroxide.fbRdfPartitioner.PartitionFreebaseRDFApp;
import com.ontology2.hydroxide.turtleOne.ConstructTurtleOneApp.ConstructTurtleOne;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.rdf.JenaUtil;

public class ParallelSuperEyeballApp extends CommandLineApplication {

	public class SortGoodAndBadTriples implements Millipede<PrimitiveTriple> {

		private final TripleMultiFile accepted;
		private final LineMultiFile<PrimitiveTriple> rejected;

		public SortGoodAndBadTriples(TripleMultiFile accepted,
				LineMultiFile<PrimitiveTriple> rejected) {
			this.accepted=accepted;
			this.rejected=rejected;
		}

		@Override
		public Sink<PrimitiveTriple> createSegment(int segmentNumber)
				throws Exception {
			final Sink<Triple> acceptBin=accepted.createSink(segmentNumber);
			final Sink<PrimitiveTriple> rejectBin=rejected.createSink(segmentNumber);
			
			return new Sink<PrimitiveTriple>() {

				@Override
				public void accept(PrimitiveTriple obj) throws Exception {
					try {
						Node_URI subject=(Node_URI) JenaUtil.ParseNode(obj.subject);
						Node_URI predicate=(Node_URI) JenaUtil.ParseNode(obj.predicate);
						Node object=JenaUtil.ParseNode(obj.object);
						acceptBin.accept(new Triple(subject,predicate,object));
					} catch(Exception e) {
						rejectBin.accept(obj);
					}
				}

				@Override
				public void close() throws Exception {
					acceptBin.close();
					rejectBin.close();
				}
				
			};
		}

	}

	public ParallelSuperEyeballApp(String[] arguments) {
		super(arguments);
	}

	public static void main(String[] args) {
		new ParallelSuperEyeballApp(args).run();
	}
	
	@Override
	protected void _run() throws Exception {
		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);	
		LineMultiFile<PrimitiveTriple> input=PartitionsAndFiles.getPartitionedExpandedTriples();
		TripleMultiFile accepted=PartitionsAndFiles.getBaseKBLime();
		LineMultiFile<PrimitiveTriple> rejected=PartitionsAndFiles.getBaseKBLimeRejected();
		
		if(accepted.testExists() || rejected.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Millipede<PrimitiveTriple> pipeline=new SortGoodAndBadTriples(accepted,rejected);

		Runner<PrimitiveTriple> runner = new Runner<PrimitiveTriple>(input,pipeline);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}

	
}
