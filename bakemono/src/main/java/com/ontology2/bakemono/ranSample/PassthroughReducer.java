package com.ontology2.bakemono.ranSample;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PassthroughReducer<K,V> extends Reducer<K,V,K,V> {
    
    @Override
    protected void reduce(K key, Iterable<V> value,
            Context context)
            throws IOException, InterruptedException {
        for(V v:value) {
            context.write(key,null);
        };
    }
}
