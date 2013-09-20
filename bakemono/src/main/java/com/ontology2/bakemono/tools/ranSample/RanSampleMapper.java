package com.ontology2.bakemono.tools.ranSample;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;

import com.ontology2.bakemono.sieve3.Sieve3Configuration.Rule;
import com.ontology2.centipede.primitiveTriples.PrimitiveTriple;

public class RanSampleMapper extends Mapper <LongWritable,Text,Text,LongWritable> {
    final public static String P="com.ontology2.bakemono.ranSample.p";
    
    double p;  // probability that we accept any given triple
    final Random generator=new Random();
    
    @Override
    public void setup(Context context) {
        p=Double.parseDouble(context.getConfiguration().get(P));
    }
    
    @Override
    public void map(LongWritable arg0, Text line, Context c) throws IOException, InterruptedException {
        if(generator.nextDouble()<p) {
            c.write(line,null);  // passing "null" means the TextOutputFormat will just print the line 
        }
    }
    
}
