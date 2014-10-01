package com.ontology2.bakemono.jena;

import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionOnSubject extends Partitioner<WritableTriple,WritableTriple> {

    @Override
    public int getPartition(WritableTriple key, WritableTriple value, int i) {
        return key.getTriple().getSubject().toString().hashCode() % i;
    }
}
