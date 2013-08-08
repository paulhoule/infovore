package com.ontology2.bakemono.primitiveTriple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

abstract class LineProcessingRecordReader<X> extends
        RecordReader<LongWritable, X> {
    LineRecordReader innerReader;

    public LineProcessingRecordReader() {
        innerReader=new LineRecordReader();
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        innerReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
            InterruptedException {
        return innerReader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return innerReader.getCurrentKey();
    }

    @Override
    public X getCurrentValue() throws IOException,
            InterruptedException {
        Text line=innerReader.getCurrentValue();
        return convert(line);
    }

    abstract X convert(Text line);

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return innerReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        innerReader.close();
    }
}