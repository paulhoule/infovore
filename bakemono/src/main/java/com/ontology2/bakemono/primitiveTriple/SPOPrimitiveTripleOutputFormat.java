package com.ontology2.bakemono.primitiveTriple;

import org.apache.hadoop.io.LongWritable;

import com.ontology2.centipede.primitiveTriples.PrimitiveTriple;

public class SPOPrimitiveTripleOutputFormat extends PrimitiveTripleOutputFormat<PrimitiveTriple,LongWritable> {

    @Override
    protected PrimitiveTriple makeTriple(PrimitiveTriple key, LongWritable value) {
        return key;
    }

}
