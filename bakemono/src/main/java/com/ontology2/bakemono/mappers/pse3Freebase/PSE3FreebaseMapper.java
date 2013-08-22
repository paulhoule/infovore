package com.ontology2.bakemono.mappers.pse3Freebase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mappers.freebasePrefilter.FreebaseRDFMapper;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;

/**
 * This class attempts to do what the ChainMapper ought to be doing.  We're probably
 * going to cascading because Hadoop forces us to do something like this but I want
 * to make sure this idea works
 *
 */
public class PSE3FreebaseMapper extends Mapper<LongWritable,Text,WritableTriple,LongWritable> {
    FreebaseRDFMapper map1;
    PSE3Mapper map2;
    
    @Override public void setup(Context job) {
        map1 = new FreebaseRDFMapper();
        map2 = new PSE3Mapper();
        
 //       //awful setup (1) I don't actually the the context
 //       map1.setup(null);
//        map2.setup(null);
    }
}
