package com.ontology2.bakemono;

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
	
	@Override
	public void close() throws IOException, InterruptedException {
		mos.close();
	};
}
