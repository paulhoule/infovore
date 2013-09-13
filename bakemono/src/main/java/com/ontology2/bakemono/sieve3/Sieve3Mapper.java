package com.ontology2.bakemono.sieve3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ontology2.bakemono.jena.WritableTriple;

public class Sieve3Mapper extends Mapper<LongWritable,WritableTriple,WritableTriple,LongWritable> {

}
