package com.ontology2.hydroxide.cutLite;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.hydroxide.files.PartitionsAndFiles;
import com.ontology2.millipede.TripleMultiFile;
import com.ontology2.millipede.pop.Millipede;
import com.ontology2.millipede.pop.Runner;
import com.ontology2.millipede.sink.Accumulator;
import com.ontology2.millipede.sink.Sink;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class ComputeSizeStatisticsApp {
	
	public static class StatisticsResult {
		public final int linkCount;
		public final long maxMid;
		
		public StatisticsResult(int linkCount, long maxMid) {
			this.linkCount = linkCount;
			this.maxMid = maxMid;
		}
		
		public StatisticsResult() {
			this(0,0);
		}
		
		public StatisticsResult reduce(StatisticsResult that) {
			return new StatisticsResult(
					this.linkCount+that.linkCount,
					Math.max(this.maxMid,that.maxMid)
			);
		}
	}
	
	
	public static class ComputeSizeStatisticsSegment implements Sink<Triple>,Accumulator {

		int linkCount=0;
		long maxMid=0;
		
		@Override
		public void accept(Triple obj) throws Exception {
			if(obj.getObject().isURI()) {
				linkCount++;
			}

			String fbMid=toFb(obj.getSubject());
			long midValue=midToLong(fbMid);
			maxMid=(midValue>maxMid) ? midValue : maxMid;
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public Object getResult() {
			return new StatisticsResult(linkCount,maxMid);
		}

	}

	public static class ComputeSizeStatistics implements Millipede<Triple> {


		public ComputeSizeStatistics() {
			// TODO Auto-generated constructor stub
		}

		@Override
		public Sink<Triple> createSegment(int segmentNumber) throws Exception {
			return new ComputeSizeStatisticsSegment();
		}

	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	
	public static void main(String[] args) throws Exception {
		TripleMultiFile in=PartitionsAndFiles.getTurtleThree();
		Millipede<Triple> mp=new ComputeSizeStatistics();
		Runner r=new Runner(in,mp);
		r.setNThreads(PartitionsAndFiles.getNThreads());
		List<Object> o=r.run();
		StatisticsResult result=new StatisticsResult();
		for(Object item:o) {
			result=result.reduce((StatisticsResult) item);
		}
		
		System.out.println("Number of links:"+result.linkCount);
		System.out.println("Maximum mid:"+result.maxMid);
	}

}
