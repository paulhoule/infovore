package com.ontology2.bakemono.primitiveTriples;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

abstract class LineProcessingRecordReader<X> extends
        RecordReader<LongWritable, X> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(LineProcessingRecordReader.class);
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
        boolean b=innerReader.nextKeyValue();
        return b;
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