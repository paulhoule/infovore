package com.ontology2.bakemono;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;


public class PrimaryKeyValueAcceptor<K,V> implements KeyValueAcceptor<K,V> {

	private final Context innerContext;  // will this ALWAYS be valid?
	
	public PrimaryKeyValueAcceptor(Context innerContext) {
		this.innerContext = innerContext;
	}

	public void write(K k, V v) throws IOException, InterruptedException {
		innerContext.write(k,v);
	}

}
