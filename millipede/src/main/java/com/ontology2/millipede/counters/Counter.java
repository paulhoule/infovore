package com.ontology2.millipede.counters;

public interface Counter {
	//
	// a counter face is specific to a bin so that different threads
	// could see a different object
	//
	
	public CounterFace getFace(int binNumber);
	public long getCount();
}
