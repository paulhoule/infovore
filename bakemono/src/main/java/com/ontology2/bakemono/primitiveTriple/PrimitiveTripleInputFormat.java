package com.ontology2.bakemono.primitiveTriple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

public class PrimitiveTripleInputFormat extends FileInputFormat<LongWritable,PrimitiveTriple> {

    @Override
    public RecordReader<LongWritable, PrimitiveTriple> createRecordReader(
            final InputSplit split, final TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new LineProcessingRecordReader() {

            @Override
            Object convert(Text line) {
                // TODO Auto-generated method stub
                return null;
            }
            
        };
    }

}
