package com.ontology2.bakemono.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.google.common.collect.Lists;

public class RealMultipleOutputsCommitter extends OutputCommitter {
    
    public List<OutputCommitter> committers;

    //
    // TODO: Make the lists below immutable,  widen List<> in constructor
    //
    
    public RealMultipleOutputsCommitter(OutputCommitter outputCommitter) {
        this.committers=Lists.newArrayList(outputCommitter);
    }

    public RealMultipleOutputsCommitter(List<OutputCommitter> committers) {
        this.committers=committers;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
        for(OutputCommitter that:committers)
            that.setupJob(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
        for(OutputCommitter that:committers)
            that.setupTask(taskContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
            throws IOException {
        for(OutputCommitter that:committers)
            if(that.needsTaskCommit(taskContext))
                return true;
        
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
        for(OutputCommitter that:committers)
            that.commitTask(taskContext);
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
        for(OutputCommitter that:committers)
            that.abortTask(taskContext);
    }

}
