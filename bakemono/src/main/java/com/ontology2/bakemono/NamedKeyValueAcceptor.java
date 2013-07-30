package com.ontology2.bakemono;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NamedKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {

	private final Context innerContext;  // will this ALWAYS be valid?
	private final String name;
	
	private MultipleOutputs mos;
	
	public NamedKeyValueAcceptor(Context innerContext, String name) {
		this.innerContext = innerContext;
		this.name = name;
		mos=new MultipleOutputs(innerContext);
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
