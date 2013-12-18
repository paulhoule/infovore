package com.ontology2.bakemono.entityCentric;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

abstract public class EntityMatchesRuleReducer<KEY,VALUE> extends Reducer<KEY,VALUE,NullWritable,VALUE> {
    static Logger log= Logger.getLogger(EntityMatchesRuleReducer.class);

    @Override
    protected void reduce(KEY key,Iterable<VALUE> values,Context context) throws IOException, InterruptedException {
        
        // I'm not trusting that Hadoop is honoring the contract for Iterable,  that is,
        // I can't iterate on it twice safely.
        //
        // memory consumption and speed could get a factor of 2 by adapting this so that once match returns,
        // it dumps the content of the List and streams the rest of the facts
        //
        
        List<VALUE> rewindableValues= Lists.newArrayList();
        for(VALUE value:values) {
            rewindableValues.add(copy(value));
        }

        if(matches(key,rewindableValues)) 
            for(VALUE value:rewindableValues)
                context.write(null,value);
    }

    protected abstract VALUE copy(VALUE value);

    protected abstract boolean matches(KEY key, Iterable<VALUE> values);
}
