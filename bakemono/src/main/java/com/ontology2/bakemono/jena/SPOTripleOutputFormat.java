package com.ontology2.bakemono.jena;

import org.apache.hadoop.io.LongWritable;

import com.hp.hpl.jena.graph.Triple;

public class SPOTripleOutputFormat extends TripleOutputFormat<Triple,LongWritable> {

    @Override
    protected Triple makeTriple(Triple key, LongWritable value) {
        return key;
    }

}
