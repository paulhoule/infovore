package com.ontology2.bakemono.mappers.pse3Freebase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ontology2.bakemono.jena.WritableTriple;

public class PSE3FreebaseMapper extends Mapper<LongWritable,Text,WritableTriple,LongWritable> {
    
}
