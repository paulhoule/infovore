package com.ontology2.bakemono.uniq;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Uniq<T> extends Reducer<T,LongWritable,T,LongWritable> {

    public static final LongWritable ONE = new LongWritable(1);

    @Override
    protected void reduce(T key, Iterable<LongWritable> value,
                          Context context)
            throws IOException, InterruptedException {
        context.write(key, null);
        incrementCounter(context,UniqCounters.DISTINCT_KEYS,1);
        for(LongWritable v:value) {
            incrementCounter(context,UniqCounters.TOTAL_VALUES,1);
        };
    }

    //
    // this code prevents failing test because the mock object Context we are passing back
    // always returns null from getCounter...  With a more sophisticated mock object perhaps
    // the system will produce individual mocks for each counter so we can watch what
    // happens with counters
    //

    private void incrementCounter(Context context,Enum <?> counterId,long amount) {
        Counter counter=context.getCounter(counterId);
        if(counter!=null) {
            counter.increment(amount);
        };
    };
}
