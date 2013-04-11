package com.ontology2.millipede.pop;

import com.google.common.collect.Lists;
import com.ontology2.millipede.MultiFile;
import com.ontology2.millipede.MultiSource;
import com.ontology2.millipede.Plumbing;
import com.ontology2.millipede.counters.Counter;
import com.ontology2.millipede.counters.SimpleCounter;
import com.ontology2.millipede.sink.Accumulator;
import com.ontology2.millipede.sink.Sink;
import com.ontology2.millipede.source.Source;

import java.util.List;
import java.util.concurrent.*;

import org.apache.log4j.Logger;

public class Runner<InT> {
	static Logger logger = Logger.getLogger(Runner.class);
	
	final MultiSource<InT> input;
	final Millipede<InT> millipede;
	final private Counter inputFacts=new SimpleCounter();
	
	int nThreads=0;

	protected ExecutorService threadPool;
	protected int failedSegment=-1;
	protected Exception innerException=null;
	
	
	
	public Runner(
			MultiSource<InT> input,
			Millipede<InT> millipede) {
		this.input = input;
		this.millipede = millipede;
	}
	
	public void setNThreads(int nThreads) {
		this.nThreads=nThreads;
	}
	
	
	public List<Object> run() throws Exception {
		List<Future> futures=Lists.newArrayListWithCapacity(input.getPartitionCount());
		int nrOfProcessors=
			(nThreads==0) ? Runtime.getRuntime().availableProcessors() : nThreads;
			
		threadPool = Executors.newFixedThreadPool(nrOfProcessors);
		for(int i=0;i<input.getPartitionCount();i++) {
			futures.add(threadPool.submit(new RunSegment(i)));
			
		}
		threadPool.shutdown();
		threadPool.awaitTermination(365L, TimeUnit.DAYS);
		if (null!=innerException) {
			logger.warn("Failure in segment "+failedSegment,innerException);
			throw new Exception("Failure in segment "+failedSegment,innerException);
		}
		List<Object> obj=Lists.newArrayListWithCapacity(input.getPartitionCount());
		for(Future f:futures) {
			obj.add(f.get());
		}
		
		return obj;
	}
	
	
	public Object runSegment(int segmentNumber) throws Exception {
		try {
			Sink<InT> segment=millipede.createSegment(segmentNumber);
			logger.info("Starting segment #"+segmentNumber);
			long start = System.currentTimeMillis();
			input.pushBin(segmentNumber, segment);
			long elapsedTimeMillis = System.currentTimeMillis()-start;
			float elapsedTimeSec = elapsedTimeMillis/1000F;
			logger.info("Completed segment #"+segmentNumber+" in time "+elapsedTimeSec+" s");
	
			if(segment instanceof Accumulator) {
				return ((Accumulator) segment).getResult();
			}
		} catch(Exception e) {
			synchronized(this) {
				if (failedSegment==-1) {
					failedSegment=segmentNumber;
				    innerException=e;
				}
			}
			threadPool.shutdownNow();

		}
		
		return null;
	}


	public class RunSegment implements Callable {

		final int segmentNumber;

		public RunSegment(int segmentNumber) {
			this.segmentNumber = segmentNumber;
		}

		@Override
		public Object call() throws Exception {
			try {
				return runSegment(segmentNumber);
			} catch(Exception e) {
				e.printStackTrace();
				return e;
			}
			
		}
	}
}
