package com.ontology2.bakemono.mapred;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

public class RealMultipleOutputsCommitterTest {

    private OutputCommitter rootCommitter;
    private RealMultipleOutputsCommitter committer;
    private JobContext jobContext;
    private TaskAttemptContext taskAttemptContext;

    @Before
    public void setup() {
        rootCommitter=mock(OutputCommitter.class);
        committer=new RealMultipleOutputsCommitter(rootCommitter);
        jobContext=mock(JobContext.class);
        taskAttemptContext=mock(TaskAttemptContext.class);
    }
    
    @Test
    public void testSetupJob() throws IOException {
        committer.setupJob(jobContext);
        verify(rootCommitter).setupJob(jobContext);
        verifyNoMoreInteractions(rootCommitter);
    }
    
    @Test
    public void testSetupTask() throws IOException {
        committer.setupTask(taskAttemptContext);
        verify(rootCommitter).setupTask(taskAttemptContext);
        verifyNoMoreInteractions(rootCommitter);
    }

    @Test
    public void testPositiveTaskCommitCase() throws IOException {
        when(rootCommitter.needsTaskCommit(taskAttemptContext)).thenReturn(true);
        assertTrue(committer.needsTaskCommit(taskAttemptContext));
    }
    
    @Test
    public void testNegativeTaskCommitCase() throws IOException {
        when(rootCommitter.needsTaskCommit(taskAttemptContext)).thenReturn(false);
        assertFalse(committer.needsTaskCommit(taskAttemptContext));
    }
    
    @Test
    public void testCommitTask() throws IOException {
        committer.commitTask(taskAttemptContext);
        verify(rootCommitter).commitTask(taskAttemptContext);
        verifyNoMoreInteractions(rootCommitter);       
    }
    
    @Test
    public void testAbortTask() throws IOException {
        committer.abortTask(taskAttemptContext);
        verify(rootCommitter).abortTask(taskAttemptContext);
        verifyNoMoreInteractions(rootCommitter);       
    }
}
