package com.ontology2.bakemono.entityCentric;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

abstract public class EntityMatchesRuleReducer<KEY,VALUE> extends Reducer<KEY,VALUE,NullWritable,VALUE> {
    @Override
    protected void reduce(KEY key,Iterable<VALUE> values,Context context) throws IOException, InterruptedException {
//        if(matches(key,values))
            for(VALUE value:values)
                context.write(null,value);
    }

    protected abstract boolean matches(KEY key, Iterable<VALUE> values);
}
