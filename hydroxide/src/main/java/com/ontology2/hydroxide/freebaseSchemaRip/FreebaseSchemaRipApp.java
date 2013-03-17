package com.ontology2.hydroxide.freebaseSchemaRip;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.parallelSuperEyeball.ParallelSuperEyeballApp;
import com.ontology2.hydroxide.parallelSuperEyeball.ParallelSuperEyeballApp.SortGoodAndBadTriples;
import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.sink.Sink;

public class FreebaseSchemaRipApp
	extends CommandLineApplication {

	public class ExtractSchemaTriples implements Millipede<Triple> {

		private final TripleMultiFile output;

		public ExtractSchemaTriples(TripleMultiFile output) {
			this.output=output;
		}

		@Override
		public Sink<Triple> createSegment(int segmentNumber)
				throws Exception {
			final Sink<Triple> outputBin=output.createSink(segmentNumber);
			
			return new Sink<Triple>() {

				@Override
				public void accept(Triple that) throws Exception {
					String value=that.getSubject().toString();
					if(!value.startsWith("http://rdf.freebase.com/ns/m.")) {
						outputBin.accept(that);
					}
					
				}

				@Override
				public void close() throws Exception {
					outputBin.close();
					
				}
				
			};
		}

	}
	
	@Override
	protected void _run(String[] args) throws Exception {
		PartitionPrimitiveTripleOnSubject partitionFunction=new PartitionPrimitiveTripleOnSubject(1024);	
		TripleMultiFile input=PartitionsAndFiles.getBaseKBLime();
		TripleMultiFile output=PartitionsAndFiles.getBaseKBFsr();
	
		Millipede<Triple> pipeline=new ExtractSchemaTriples(output);

		Runner<Triple> runner = new Runner<Triple>(input,pipeline);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
	}
}
