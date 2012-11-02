package com.ontology2.millipede;

public class DummyPartitionFunction<T> implements PartitionFunction<T> {

	//
	// This partition function isn't meant to be used to actually partition
	// things,  but exists because the current architecture requires us to
	// specify a partition function when we don't really need one because
	// the data is already partitioned
	//
	// This really should put things into 
	//
	
	private final int partitionCount;

	
	public DummyPartitionFunction(int partitionCount) {
		this.partitionCount = partitionCount;
	}

	@Override
	public int getPartitionCount() {
		return partitionCount;
	}

	@Override
	public int bin(T obj) {
		return obj.hashCode() % partitionCount;
	}

}
