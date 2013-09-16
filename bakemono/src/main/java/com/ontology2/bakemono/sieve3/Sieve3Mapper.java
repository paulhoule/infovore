package com.ontology2.bakemono.sieve3;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.abstractions.NamedKeyValueAcceptor;
import com.ontology2.bakemono.abstractions.PrimaryKeyValueAcceptor;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mappers.pse3.PSE3Counters;
import com.ontology2.bakemono.mapred.RealMultipleOutputs;
import com.ontology2.centipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.sieve3.Sieve3Configuration.Rule;

public class Sieve3Mapper extends Mapper<LongWritable,PrimitiveTriple,PrimitiveTriple,LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    
    Sieve3Configuration sieve3conf;
    RealMultipleOutputs mos;
    KeyValueAcceptor<PrimitiveTriple,LongWritable> other;
    Map<String,KeyValueAcceptor<PrimitiveTriple,LongWritable>> outputs=Maps.newHashMap();
    
    @Override
    public void setup(Context context) throws IOException,
    InterruptedException {
        mos=new RealMultipleOutputs(context);
        super.setup(context);
        other=new PrimaryKeyValueAcceptor(context);
        sieve3conf=Sieve3Tool.generateConfiguration();
        for(Rule r:sieve3conf.getRules())
            outputs.put(r.getOutputName(), new NamedKeyValueAcceptor(mos,r.getOutputName()));
    }
    
    //
    // We might be able to ditch a hashtable lookup here to speed things up
    //
    
    @Override
    public void map(LongWritable arg0, PrimitiveTriple row3, Context c) throws IOException, InterruptedException {
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
