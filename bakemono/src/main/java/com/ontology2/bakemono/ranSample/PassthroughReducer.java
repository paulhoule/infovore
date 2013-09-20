package com.ontology2.bakemono.ranSample;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.ontology2.bakemono.jena.WritableTriple;

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
