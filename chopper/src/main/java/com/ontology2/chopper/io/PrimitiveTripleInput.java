package com.ontology2.chopper.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.primitiveTriple.PrimitiveTripleInputFormat;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

public class PrimitiveTripleInput extends LoadFunc implements LoadMetadata {

    private RecordReader<LongWritable,PrimitiveTriple> in;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    public PrimitiveTripleInput() {
        super();
    }
    
    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PrimitiveTripleInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        in = reader;
        
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean done = !in.nextKeyValue();
            if (done) {
                return null;
            }
            
            PrimitiveTriple t=in.getCurrentValue();
            List<String> parts=new ArrayList<String>(3);
            parts.add(t.subject);
            parts.add(t.predicate);
            parts.add(t.object);
            
            return mTupleFactory.newTupleNoCopy(parts);

        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }

    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        return new ResourceSchema().setFields(new ResourceFieldSchema[] {
                new ResourceFieldSchema().setName("s").setType(DataType.CHARARRAY),
                new ResourceFieldSchema().setName("p").setType(DataType.CHARARRAY),
                new ResourceFieldSchema().setName("o").setType(DataType.CHARARRAY)
        });
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        
    }

}
