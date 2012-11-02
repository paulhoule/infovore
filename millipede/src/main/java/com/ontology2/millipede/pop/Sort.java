package com.ontology2.millipede.pop;

import java.util.Comparator;

import com.ontology2.millipede.LineMultiFile;
import com.ontology2.millipede.sink.Sink;

public class Sort<T> implements Millipede<T> {

	private final Comparator<? super T> comparator;
	private final Millipede<T> output;
	
	public Sort(Comparator<? super T> comparator,Millipede<T> output) {
		this.comparator = comparator;
		this.output = output;
	}

	@Override
	public Sink<T> createSegment(int segmentNumber) throws Exception {
		return new SortSegment<T>(comparator,output.createSegment(segmentNumber));
	}
}
