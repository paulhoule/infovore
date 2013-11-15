package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedKeyPartitioner<T extends WritableComparable> extends Partitioner<TaggedKey<T>,Writable> {
    @Override
    public int getPartition(TaggedKey<T> key, Writable writable, int n) {
        return (key.getKey().hashCode() & 0x7FFFFFFF) % n;
    };
}
