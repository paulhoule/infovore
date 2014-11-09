package com.ontology2.bakemono.jena;

import org.apache.hadoop.mapreduce.Partitioner;

import static java.lang.Math.abs;

public class PartitionOnSubject extends Partitioner<WritableTriple,WritableTriple> {

    @Override
    public int getPartition(WritableTriple key, WritableTriple value, int i) {
        return abs(key.getTriple().getSubject().toString().hashCode()) % i;
    }
}
