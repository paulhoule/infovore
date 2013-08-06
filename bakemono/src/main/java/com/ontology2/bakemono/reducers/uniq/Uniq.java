package com.ontology2.bakemono.reducers.uniq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Uniq<K> extends Reducer<K, LongWritable, K, LongWritable> {

    @Override
    protected void reduce(K key, Iterable<LongWritable> value,
            Context context)
            throws IOException, InterruptedException {
        context.write(key, new LongWritable(1));

    }
    

}
