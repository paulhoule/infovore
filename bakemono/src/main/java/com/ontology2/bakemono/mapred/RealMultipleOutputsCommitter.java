package com.ontology2.bakemono.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class RealMultipleOutputsCommitter extends OutputCommitter {
    
    public OutputCommitter rootCommitter;

    public RealMultipleOutputsCommitter(OutputCommitter outputCommitter) {
        this.rootCommitter=outputCommitter;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
        rootCommitter.setupJob(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
        rootCommitter.setupTask(taskContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
            throws IOException {
        return rootCommitter.needsTaskCommit(taskContext);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
        rootCommitter.commitTask(taskContext);
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
        rootCommitter.abortTask(taskContext);
    }

}
