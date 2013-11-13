package com.ontology2.bakemono.setOperations;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

//
// How this is used:
//
// Set Members are of <Type>,  and the identity of the set is encoded as a
// VIntWritable,  which is either 1 or 2.
//
// We're performing the set substraction S_1 - S_2,  so we are fetching elements
// of set one that are not members of set two.
//
//
public class SetDifferenceReducer<Type> extends Reducer<Type,VIntWritable,Type,NullWritable> {

}
