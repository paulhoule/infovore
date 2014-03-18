package com.ontology2.haruhi.flows;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"../shell/applicationContext.xml","../shell/testDefaults.xml"})

public class TestFlowBeans {
    private static Log logger = LogFactory.getLog(TestFlowBeans.class);
    @Autowired SpringFlow basekbNowFlow;
    
    @Test public void basekbNowFlowConfiguresStepsCorrectly() {
        List<String> flowArgs=Lists.newArrayList("s3n://freebase-dumps/","1942-12-07-00-00","s3n://basekb-now/");
        List<FlowStep> steps=basekbNowFlow.generateSteps(flowArgs);
        assertNotNull(steps);
        assertEquals(6,steps.size());
        
        Map<String, Object> local = Maps.newHashMap();
        int j=0;
        {
            assertTrue(steps.get(j) instanceof AssignmentStep);
            AssignmentStep that=(AssignmentStep) steps.get(j++);
            local=that.process(local, flowArgs);
        }
        
        {
            assertTrue(steps.get(j) instanceof AssignmentStep);
            AssignmentStep that=(AssignmentStep) steps.get(j++);
            local=that.process(local, flowArgs);
        }
        
        {
            assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step0=(SpringStep) steps.get(j++);
            List<String> args=step0.getStepArgs(local,flowArgs);
            
            assertEquals(4,args.size());
            
            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("freebaseRDFPrefilter",args.get(i++));
            assertEquals("s3n://freebase-dumps/freebase-rdf-1942-12-07-00-00/",args.get(i++));
            assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
        }
        
        {
            assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step1=(SpringStep) steps.get(j++);
            List<String> args=step1.getStepArgs(local,flowArgs);
            
            assertEquals(6,args.size());
            
            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("pse3",args.get(i++));
            assertEquals("-R",args.get(i++));
            assertEquals("47",args.get(i++));
            assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
            assertEquals("s3n://basekb-now/1942-12-07-00-00/",args.get(i++));
        }
        
        {
            assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step2=(SpringStep) steps.get(j++);
            List<String> args=step2.getStepArgs(local,flowArgs);
            
            assertEquals(4,args.size());
            
            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("sieve3",args.get(i++));
            assertEquals("s3n://basekb-now/1942-12-07-00-00/accepted/",args.get(i++));
            assertEquals("s3n://basekb-now/1942-12-07-00-00/sieved/",args.get(i++));
        }
        
        {
            assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step2=(SpringStep) steps.get(j++);
            List<String> args=step2.getStepArgs(local,flowArgs);
            
            assertEquals(5,args.size());
            
            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("fs",args.get(i++));
            assertEquals("-rmr",args.get(i++));
            assertEquals("s3n://basekb-now/1942-12-07-00-00/accepted/",args.get(i++));
            assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
        }
    }

}
