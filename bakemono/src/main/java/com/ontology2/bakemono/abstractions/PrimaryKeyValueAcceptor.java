package com.ontology2.bakemono.abstractions;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PrimaryKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {


    public PrimaryKeyValueAcceptor(Mapper<?,?,K,V>.Context c) {
    }

    @Override
    public void write(K k, V v,Mapper<?,?,K,V>.Context c) throws IOException, InterruptedException {
        c.write(k,v);
    }

    @Override
    public void close(Mapper<?,?,K,V>.Context c) {
    }


}
