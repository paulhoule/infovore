package com.ontology2.millipede;

public interface PartitionFunction<T> {
    public int getPartitionCount();
    public int bin(T obj);
}
