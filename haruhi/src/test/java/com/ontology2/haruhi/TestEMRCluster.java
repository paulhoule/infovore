package com.ontology2.haruhi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ontology2.haruhi.flows.ForeachStep;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.google.common.collect.Lists;
import com.ontology2.haruhi.AmazonEMRCluster;
import com.ontology2.haruhi.flows.Flow;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"shell/applicationContext.xml","shell/testDefaults.xml"})
public class TestEMRCluster {
    @Autowired AmazonEMRCluster tinyAwsCluster;
    @Autowired Flow basekbNowFlow;
    @Autowired private StepConfig debugStep;
    @Autowired Flow foreachStepFlow;

    @Test
    public void testShortName() {
        String name=tinyAwsCluster.computeJobName(Lists.newArrayList("fe","fi","fo","fum"));
        assertEquals(name,"fe fi fo fum");
    }

    @Test
    public void testLongName() {
        List<String> abc=Lists.newArrayList();
        for(int i=0;i<1000;i++)
            abc.add("x");

        String name=tinyAwsCluster.computeJobName(Lists.newArrayList(abc));
        assertEquals(name.length(),255);
    }

    @Test
    public void checkCorrectConfigurationForBaseKBNowFlow() {
        List<String> flowArgs=Lists.newArrayList("s3n://freebase-dumps/","1942-12-07-00-00","s3n://basekb-now/");
        List<StepConfig> steps=tinyAwsCluster.createEmrSteps(
                basekbNowFlow, 
                flowArgs,
                null
        );
        assertEquals(5,steps.size());
        int j=0;
        {
            StepConfig c=steps.get(j++);
            assertEquals(debugStep,c);
        }
        {
            StepConfig c=steps.get(j++);
            HadoopJarStepConfig that=c.getHadoopJarStep();
            assertEquals(
                    Arrays.asList(
                            "run"
                            ,"freebaseRDFPrefilter"
                            ,"s3n://freebase-dumps/freebase-rdf-1942-12-07-00-00/"
                            ,"/preprocessed/1942-12-07-00-00/")
                    ,that.getArgs());
        }
        
        {
            StepConfig c=steps.get(j++);
            HadoopJarStepConfig that=c.getHadoopJarStep();
            assertEquals(
                    Arrays.asList(
                            "run"
                            ,"pse3"
                            ,"/preprocessed/1942-12-07-00-00/"
                            ,"s3n://basekb-now/1942-12-07-00-00/")
                    ,that.getArgs());
        }
        
        {
            StepConfig c=steps.get(j++);
            HadoopJarStepConfig that=c.getHadoopJarStep();
            assertEquals(
                    Arrays.asList(
                            "run"
                            ,"sieve3"
                            ,"s3n://basekb-now/1942-12-07-00-00/accepted/"
                            ,"s3n://basekb-now/1942-12-07-00-00/sieved/")
                    ,that.getArgs());
        }
        
        {
            StepConfig c=steps.get(j++);
            HadoopJarStepConfig that=c.getHadoopJarStep();
            assertEquals(
                    Arrays.asList(
                            "run"
                            ,"fs"
                            ,"-rmr"
                            ,"s3n://basekb-now/1942-12-07-00-00/accepted/"
                            ,"/preprocessed/1942-12-07-00-00/")
                    ,that.getArgs());
        }
    }

    @Test
    public void testForeachLoop() {
        List<String> flowArgs=Lists.newArrayList("nellyF");
        List<StepConfig> steps=tinyAwsCluster.createEmrSteps(
                foreachStepFlow,
                flowArgs,
                null
        );

        assertEquals(7,steps.size());
        assertEquals(Arrays.asList("doItForYear","2000"),steps.get(1).getHadoopJarStep().getArgs());
        assertEquals(Arrays.asList("nellyF","2000"),steps.get(2).getHadoopJarStep().getArgs());
        assertEquals(Arrays.asList("doItForYear","2001"),steps.get(3).getHadoopJarStep().getArgs());
        assertEquals(Arrays.asList("nellyF","2001"),steps.get(4).getHadoopJarStep().getArgs());
        assertEquals(Arrays.asList("doItForYear","2002"),steps.get(5).getHadoopJarStep().getArgs());
        assertEquals(Arrays.asList("nellyF","2002"),steps.get(6).getHadoopJarStep().getArgs());
    }
}
