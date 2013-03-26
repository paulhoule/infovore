package com.ontology2.hydroxide.parallelSuperEyeball;

import com.google.common.cache.LoadingCache;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.fbRdfPartitioner.PartitionFreebaseRDFApp;
import com.ontology2.hydroxide.files.InputFileConstellation;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.files.StandardFileConstellation;
import com.ontology2.hydroxide.turtleOne.ConstructTurtleOneApp.ConstructTurtleOne;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
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
			final LoadingCache<String,Node> nodeParser=JenaUtil.createNodeParseCache();
			
			return new Sink<PrimitiveTriple>() {

				@Override
				public void accept(PrimitiveTriple obj) throws Exception {
					try {					
						Node_URI subject=(Node_URI) nodeParser.get(obj.subject);
						Node_URI predicate=(Node_URI) nodeParser.get(obj.predicate);
						Node object=nodeParser.get(obj.object);
						acceptBin.accept(new Triple(subject,predicate,object));
					} catch(Throwable e) {
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
	
	@Override
	protected void _run(String[] args) throws Exception {

		if(args.length<1) {
			die("parallelSuperEyeball [project name]");
		}
	
		for(String project:args) {
			pseOneProject(project);
		}
	}

	private void pseOneProject(String projectName) throws Exception {
		StandardFileConstellation files = new StandardFileConstellation(projectName);

		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);	
		LineMultiFile<PrimitiveTriple> input=files.getPartitionedExpandedTriples();
		TripleMultiFile accepted=files.getDestination();
		LineMultiFile<PrimitiveTriple> rejected=files.getDestinationRejected();
		
		if(accepted.testExists() || rejected.testExists()) {
			throw new Exception("Destination files already exist");	
		}
		
		Millipede<PrimitiveTriple> pipeline=new SortGoodAndBadTriples(accepted,rejected);

		Runner<PrimitiveTriple> runner = new Runner<PrimitiveTriple>(input,pipeline);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}	
}
