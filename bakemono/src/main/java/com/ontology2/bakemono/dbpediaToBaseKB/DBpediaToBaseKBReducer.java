package com.ontology2.bakemono.dbpediaToBaseKB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DBpediaToBaseKBReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values)
            context.write(key,value);
    }
}
