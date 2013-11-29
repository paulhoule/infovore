package com.ontology2.bakemono.sieve3;

import java.io.IOException;
import java.util.Map;

import com.ontology2.bakemono.abstractions.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mapred.RealMultipleOutputs;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.bakemono.pse3.PSE3Counters;
import com.ontology2.bakemono.sieve3.Sieve3Configuration.Rule;

public class Sieve3Mapper extends Mapper<LongWritable,Text,PrimitiveTriple,LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    
    Sieve3Configuration sieve3conf;
    RealMultipleOutputs mos;
    KeyValueAcceptor<PrimitiveTriple,LongWritable> other;
    Map<String,KeyValueAcceptor<PrimitiveTriple,LongWritable>> outputs=Maps.newHashMap();

    private ApplicationContext applicationContext;
    final static Codec<PrimitiveTriple> primitiveTripleCodec=new PrimitiveTripleCodec();
    
    @Override
    public void setup(Context context) throws IOException,
    InterruptedException {
        applicationContext=Spring.getApplicationContext(context.getConfiguration());
        mos=new RealMultipleOutputs(context);
        super.setup(context);
        other=new PrimaryKeyValueAcceptor(context);
        sieve3conf = Sieve3Tool.createDefaultConfiguration(applicationContext);
        
        for(Rule r:sieve3conf.getRules())
            outputs.put(r.getOutputName(), new NamedKeyValueAcceptor(mos,r.getOutputName()));
        
    }
    
    //
    // We might be able to ditch a hashtable lookup here to speed things up
    //
    
    @Override
    public void map(LongWritable arg0, Text line, Context c) throws IOException, InterruptedException {
        PrimitiveTriple row3=primitiveTripleCodec.decode(line.toString());
        for(Rule r:sieve3conf.getRules())
            if(r.getCondition().apply(row3)) {
                outputs.get(r.getOutputName()).write(row3,ONE,c);
                return;
            }

        other.write(row3, ONE, c);
    }
    
    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
