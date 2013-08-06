package com.ontology2.bakemono.reducers.uniq;

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;

public class Uniq extends Reducer<WritableTriple, LongWritable, Triple, LongWritable> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(Uniq.class);
    
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(WritableTriple key, Iterable<LongWritable> value,
            Context context)
            throws IOException, InterruptedException {
        context.write(key.getTriple(), new LongWritable(1));
    }
    

}
