package com.ontology2.millipede;

import com.ontology2.millipede.sink.Sink;

public interface MultiSource<T> {
    public int getPartitionCount();
    public long pushBin(int binNumber,Sink<T> destination) throws Exception;
}
