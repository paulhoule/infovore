package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Triple;
import org.apache.hadoop.io.LongWritable;

public class SPOTripleOutputFormat extends TripleOutputFormat<Triple,LongWritable> {

    @Override
    protected Triple makeTriple(Triple key, LongWritable value) {
        return key;
    }

}
