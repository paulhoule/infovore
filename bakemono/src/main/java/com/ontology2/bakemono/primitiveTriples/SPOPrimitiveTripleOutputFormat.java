package com.ontology2.bakemono.primitiveTriples;

import org.apache.hadoop.io.LongWritable;

public class SPOPrimitiveTripleOutputFormat extends PrimitiveTripleOutputFormat<PrimitiveTriple,LongWritable> {

    @Override
    protected PrimitiveTriple makeTriple(PrimitiveTriple key, LongWritable value) {
        return key;
    }

}
