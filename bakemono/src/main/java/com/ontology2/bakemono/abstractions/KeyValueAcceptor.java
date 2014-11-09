package com.ontology2.bakemono.abstractions;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public interface KeyValueAcceptor<K,V> {
    public void write(K k,V v,Mapper<?,?,K,V>.Context c) throws IOException,InterruptedException;
    public void close(Mapper<?,?,K,V>.Context c) throws IOException, InterruptedException;
}
