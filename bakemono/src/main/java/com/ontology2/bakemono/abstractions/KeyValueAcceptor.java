package com.ontology2.bakemono.abstractions;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;

public interface KeyValueAcceptor<K,V> {
    public void write(K k,V v,Context c) throws IOException,InterruptedException;
    public void close(Context c) throws IOException, InterruptedException;
}
