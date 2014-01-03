package com.ontology2.bakemono;

import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.junit.Test;

import com.ontology2.bakemono.MainBase.IncorrectUsageException;

public class MainTest {

    @Test
    public void theDefaultReducerIsTheIdentityReducer() {
        JobConf conf = new JobConf(Main.class);
        assertEquals(IdentityReducer.class,conf.getReducerClass());
    }

    @Test
    public void whatIsTheDefaultPartitioner() {
        JobConf conf = new JobConf(Main.class);
        assertEquals(HashPartitioner.class,conf.getPartitionerClass());	
    }

    @Test(expected=IncorrectUsageException.class)
    public void bombsOutIfNoArguments() throws Exception {
        Main main=new Main(new String[0]);
        main.parseArguments();
    }

    @Test(expected=IncorrectUsageException.class) 
    public void bombsOutWithAribitraryArgument() throws Exception {
        Main main=new Main(new String[] {"pulverize"});
        main.parseArguments();
    }

    @Test
    public void acceptsRunCommand() throws Exception {
        Main main=new Main(new String[] {"run","freebaseRDFPrefilter"});
        main.parseArguments();
        assertEquals("freebaseRDFPrefilter",main.getToolName());
        assertTrue(main.getToolArgs().isEmpty());
    }
    
    @Test
    public void acceptsListCommand() throws Exception {
        Main main=new Main(new String[] {"list"});
        main.parseArguments();
        assertEquals(null,main.getToolName());
        assertEquals(Main.ListTools.class,main.cmd.getClass());
    }

    @Test
    public void acceptsRunCommandWithArguments() throws Exception {
        Main main=new Main(new String[] {"run","freebaseRDFPrefilter", "123", "the", "crew", "is", "called", "BDP"});
        main.parseArguments();
        assertEquals("freebaseRDFPrefilter",main.getToolName());
        assertEquals(main.getToolArgs().size(),6);
        assertEquals("called",main.getToolArgs().get(4));
    }

    @Test(expected=IncorrectUsageException.class)
    public void ButRunNeedsAnArgument() throws Exception {
        Main main=new Main(new String[] {"run"});
        main.parseArguments();
    }

    @Test(expected=IncorrectUsageException.class)
    public void AndNotJustAnyArgument() throws Exception {
        Main main=new Main(new String[] {"run","frumpkins"});
        main.parseArguments();
    }
}
