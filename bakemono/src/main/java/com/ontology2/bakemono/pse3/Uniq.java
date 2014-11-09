package com.ontology2.bakemono.pse3;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.jena.WritableTriple;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Uniq extends Reducer<WritableTriple, LongWritable, Triple, LongWritable> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(Uniq.class);
    

    @Override
    protected void reduce(WritableTriple key, Iterable<LongWritable> value,
            Context context)
            throws IOException, InterruptedException {
        context.write(key.getTriple(), new LongWritable(1));
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
