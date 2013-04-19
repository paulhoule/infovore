package com.ontology2.millipede;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import com.ontology2.millipede.counters.Counter;
import com.ontology2.millipede.sink.Sink;

public class Partitioner<T> implements Sink<T> {
	public MultiFile<T> mf;
	public List<Sink<T>> sinks;
	private long factCount;
	
	public Partitioner(MultiFile<T> mf) throws Exception {
		this.mf=mf;
		final int count = mf.getPartitionFunction().getPartitionCount();
		sinks=new ArrayList<Sink<T>>(count);
		for(int i=0;i<count;i++) {
			sinks.add(mf.createSink(i));
		}
	}

	@Override
	public void accept(T obj) throws Exception {
		int bin=mf.getPartitionFunction().bin(obj);
		sinks.get(bin).accept(obj);
		factCount++;
	}

	@Override
	public void close() throws Exception {
		final int count = mf.getPartitionFunction().getPartitionCount();
		for(int i=0;i<count;i++) {
			sinks.get(i).close();
		}
	};
	
	public long getFactCount() {
		return factCount;
	}
}
