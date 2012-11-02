package com.ontology2.tripleFilter;

import com.ontology2.millipede.sink.NonClosingSink;
import com.ontology2.millipede.sink.Sink;

public class TripleCounter extends NonClosingSink<SituatedTriple> {

	long count=0;

	public long getCount() {
		return count;
	}
	
	@Override
	public void accept(SituatedTriple obj) throws Exception {
		count++;
	}
}
