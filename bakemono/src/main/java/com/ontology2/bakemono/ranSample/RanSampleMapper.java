package com.ontology2.bakemono.ranSample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;


public class RanSampleMapper extends Mapper <LongWritable,Text,Text,LongWritable> {
    final public static String P="com.ontology2.bakemono.ranSample.p";
    final public static String NULL_VALUE="com.ontology2.bakemono.ranSample.nullValue";

    private static final LongWritable ONE = new LongWritable(1);
    
    double p;  // probability that we accept any given triple
    LongWritable constantValue;
    
    final Random generator=new Random();
    
    @Override
    public void setup(Context context) {
        p=Double.parseDouble(context.getConfiguration().get(P));
        constantValue =
                Boolean.parseBoolean(context.getConfiguration().get(NULL_VALUE))
                ? null : ONE;
        
    }
    
    @Override
    public void map(LongWritable arg0, Text line, Context c) throws IOException, InterruptedException {
        if(generator.nextDouble()<p) {
            c.write(line,constantValue);  // passing "null" means the TextOutputFormat will just print the line 
        }
    }
    
}
