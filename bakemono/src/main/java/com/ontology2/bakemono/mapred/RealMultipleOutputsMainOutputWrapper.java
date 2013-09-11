package com.ontology2.bakemono.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Lists;

public class RealMultipleOutputsMainOutputWrapper<K,V> extends FileOutputFormat<K,V> {

    private static final String ROOT_OUTPUT_FORMAT = "com.ontology2.bakemono.RealMultipleOutputsMainOutputWrapper.rootOutputFormat";

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        
        return getRootOutputFormat(job).getRecordWriter(job);
    }

    public static void setRootOutputFormat(Job job,  Class <? extends FileOutputFormat> theClass) {
        job.getConfiguration().setClass(ROOT_OUTPUT_FORMAT, theClass, FileOutputFormat.class);
    };
    
    public FileOutputFormat<K,V> getRootOutputFormat(TaskAttemptContext job) {
        if(_innerFormat==null) {
            Configuration conf = job.getConfiguration();
            Class c =conf.getClass(ROOT_OUTPUT_FORMAT,FileOutputFormat.class);
            try {
                _innerFormat=(FileOutputFormat<K,V>) c.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        
        return _innerFormat;
    }
    
    private FileOutputFormat<K,V> _innerFormat;
    private OutputCommitter _committer; 
    
    @Override
    public synchronized OutputCommitter getOutputCommitter(
            TaskAttemptContext context) throws IOException {
        if(_committer==null) {
            // insert list here?  we should have enough in the context to construct the object states...
            List<OutputCommitter> committers=Lists.newArrayList(super.getOutputCommitter(context));
            for(String name:RealMultipleOutputs.getNamedOutputsList(context))
                committers.add(new FileOutputCommitter(
                        new Path(RealMultipleOutputs.getHdfsPath(context,name)),
                        RealMultipleOutputs._getContext(context,name)
                ));
                
            _committer=new RealMultipleOutputsCommitter(committers);
        }
        
        return _committer;
    }
}
