package com.ontology2.bakemono.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;

public class TestMapper extends Mapper<LongWritable,Text,FloatWritable,BloomFilter> {
}
