package com.ontology2.millipede;

public class StubPartitionFunction implements PartitionFunction<Object> {

    private final int count;

    public StubPartitionFunction(int count) {
        this.count=count;
    }

    @Override
    public int getPartitionCount() {
        return count;
    }

    @Override
    public int bin(Object obj) {
        return obj.hashCode() % count;
    }

}
