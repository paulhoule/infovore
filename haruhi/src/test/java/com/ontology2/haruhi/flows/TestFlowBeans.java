package com.ontology2.haruhi.flows;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"../shell/applicationContext.xml","../shell/testDefaults.xml"})

public class TestFlowBeans {
    @Autowired SpringFlow basekbNowFlow;
    
    @Test public void basekbNowFlowConfiguresStepsCorrectly() {
        List<String> flowArgs=Lists.newArrayList("s3n://freebase-dumps/","1942-12-07-00-00");
        List<FlowStep> steps=basekbNowFlow.generateSteps(flowArgs);
        assertNotNull(steps);
        assertEquals(1,steps.size());
        
        assertTrue(steps.get(0) instanceof SpringStep);
        SpringStep step0=(SpringStep) steps.get(0);
        List<String> args=step0.getStepArgs(flowArgs);
        
        assertEquals(4,args.size());
        
        int i=0;
        assertEquals("run",args.get(i++));
        assertEquals("freebaseRDFPrefilter",args.get(i++));
        assertEquals("s3n://freebase-dumps/freebase-rdf-1942-12-07-00-00/",args.get(i++));
        assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
    }
}
