package com.ontology2.bakemono.primitiveTriple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.primitiveTriples.PrimitiveTripleCodec;

public class PrimitiveTripleInputFormat extends FileInputFormat<LongWritable,PrimitiveTriple> {

    final static PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();
    
    @Override
    public RecordReader<LongWritable, PrimitiveTriple> createRecordReader(
            final InputSplit split, final TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new LineProcessingRecordReader<PrimitiveTriple>() {

            @Override
            PrimitiveTriple convert(Text line) {
                return ptc.decode(line.toString());
            }
            
        };
    }

}
