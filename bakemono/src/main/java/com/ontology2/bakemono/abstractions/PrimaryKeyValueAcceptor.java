package com.ontology2.bakemono.abstractions;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;


public class PrimaryKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {


    public PrimaryKeyValueAcceptor(Context c) {
    }

    @Override
    public void write(K k, V v,Context c) throws IOException, InterruptedException {
        c.write(k,v);
    }

    @Override
    public void close(Context c) {
    }


}
