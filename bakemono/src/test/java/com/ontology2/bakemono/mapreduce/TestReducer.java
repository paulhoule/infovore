package com.ontology2.bakemono.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;

public class TestReducer extends Reducer<FloatWritable,BloomFilter,VIntWritable,DoubleWritable> {
}
