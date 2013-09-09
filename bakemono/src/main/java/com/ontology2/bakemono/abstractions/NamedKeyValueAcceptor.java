package com.ontology2.bakemono.abstractions;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.ontology2.bakemono.mapred.RealMultipleOutputs;

public class NamedKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {

    private final String name;
    private final RealMultipleOutputs mos;

    public NamedKeyValueAcceptor(final RealMultipleOutputs mos, final String name) {
        this.name = name;
        this.mos=mos;
    }

    @Override
    public void write(K k, V v,Context c) throws IOException, InterruptedException {
        mos.write(name,k,v);
    }

    //
    // the correct way to close the OutputStreams behind this object is to
    // close the mos object once in the Mapper when the framework is tearing
    // the mapper down (see PSE3Mapper.java)
    //

    @Override
    public void close(Context c) throws IOException, InterruptedException {
    };
}
