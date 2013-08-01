package com.ontology2.bakemono.abstractions;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NamedKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {

    private final String name;
    private final MultipleOutputs mos;

    public NamedKeyValueAcceptor(final MultipleOutputs mos, final String name) {
        this.name = name;
        this.mos=mos;
    }

    @Override
    public void write(K k, V v) throws IOException, InterruptedException {
        mos.write(name,k,v);
    }

    //
    // the correct way to close the OutputStreams behind this object is to
    // close the mos object once in the Mapper when the framework is tearing
    // the mapper down (see PSE3Mapper.java)
    //

    @Override
    public void close() throws IOException, InterruptedException {
    };
}
