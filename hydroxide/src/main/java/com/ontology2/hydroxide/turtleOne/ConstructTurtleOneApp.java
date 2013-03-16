package com.ontology2.hydroxide.turtleOne;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.ontology2.hydroxide.FreebaseQuad;
import com.ontology2.hydroxide.MatchesProperty;
import com.ontology2.hydroxide.MatchesPropertyAndDestination;
import com.ontology2.hydroxide.QuadCodec;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.millipede.FileOpener;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.CodecSink;
import com.ontology2.millipede.sink.LineSink;
import com.ontology2.millipede.sink.Sink;

public class ConstructTurtleOneApp{


	public static class LookupFailedException extends Exception {

	}

	public static class ConstructTurtleOne implements Millipede<FreebaseQuad> {

		private final MultiFile<FreebaseQuad> out;
		private final TurtleZero t0;
		private final CodecSink<FreebaseQuad> rejectedOut;

		public ConstructTurtleOne(TurtleZero t0, MultiFile<FreebaseQuad> out, CodecSink<FreebaseQuad> rejectedOut) {
			this.t0=t0;
			this.out=out;
			this.rejectedOut=rejectedOut;
		}

		@Override
		public Sink<FreebaseQuad> createSegment(int segmentNumber)
				throws Exception {
			return new ConstructTurtleOneSegment(out.createSink(segmentNumber));
		}
		
		public class ConstructTurtleOneSegment implements Sink<FreebaseQuad> {

			private Sink<FreebaseQuad> innerSink;

			public ConstructTurtleOneSegment(Sink<FreebaseQuad> innerSink) {
				this.innerSink=innerSink;
			}

			@Override
			public void accept(FreebaseQuad obj) throws Exception {
				try {	
					String property=t0.lookup(obj.getProperty());
					String destination = obj.getDestination().isEmpty() || "null".equals(obj.getDestination()) 
							? ""
							: t0.lookup(obj.getDestination());
					
					if (TurtleZero.FAILED.equals(property))
						throw new LookupFailedException();
					
					if (TurtleZero.FAILED.equals(destination))
						throw new LookupFailedException();
	
					innerSink.accept(new FreebaseQuad(
							obj.getSubject(),
							property,
							destination,
							obj.getValue()
					));
				
				} catch(LookupFailedException e) {
					synchronized(rejectedOut) {
						rejectedOut.accept(obj);
					}
					logger.warn("failed to load quad: "+obj);					
				}
			}

			@Override
			public void close() throws Exception {
				innerSink.close();
			}

		}

	}

	static Logger logger=Logger.getLogger(ConstructTurtleOneApp.class);
	
	public static void main(String[] args) throws Exception {
		TurtleZero t0=new TurtleZero();
		MultiFile<FreebaseQuad> in=PartitionsAndFiles.getSorted();
		MultiFile<FreebaseQuad> out=PartitionsAndFiles.getTurtleOne();
		
		String rejectedFile=PartitionsAndFiles.getTurtleOneRejectedFile();
		PrintWriter w=new FileOpener().createWriter(rejectedFile);
		LineSink s=new LineSink(w);
		CodecSink<FreebaseQuad> rejectedOut=new CodecSink(new QuadCodec(),s);
		
		Millipede<FreebaseQuad> pipeline=new ConstructTurtleOne(t0,out,rejectedOut);
		Runner<FreebaseQuad> runner = new Runner<FreebaseQuad>(in,pipeline);
		runner.setNThreads(PartitionsAndFiles.getNThreads());
		runner.run();
		rejectedOut.close();
	}
}
