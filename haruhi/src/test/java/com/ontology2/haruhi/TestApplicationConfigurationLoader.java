package com.ontology2.haruhi;

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ontology2.haruhi.flows.AssignmentStep;
import com.ontology2.haruhi.flows.FlowStep;
import com.ontology2.haruhi.flows.SpringFlow;
import com.ontology2.haruhi.flows.SpringStep;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"shell/applicationContext.xml","shell/testDefaults.xml"})
public class TestApplicationConfigurationLoader {
    // We are building this,  so the JAR ought to be in the m2 repository

    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    private ApplicationConfigurationFetcher fetcher;
    @Autowired AmazonEMRCluster tinyAwsCluster;
    @Resource
    StepConfig debugStep;

    @Test
    public void jarFileIsThere() {
        assertTrue(fetcher.testJarExists());
    }

    @Test
    public void canFindInputStream() throws IOException {
        assertNotNull(fetcher.getContextXml());
    }

    @Test
    public void checkEnrichedContext() throws IOException {
        ApplicationContext enriched=fetcher.enrichedContext();
    }

    public SpringFlow basekbNowFlow() throws IOException {
        return fetcher.enrichedContext().getBean("basekbNowFlow", SpringFlow.class);
    }
    @Test public void basekbNowFlowConfiguresStepsCorrectly() throws IOException {
        List<String> flowArgs= Lists.newArrayList("s3n://freebase-dumps/", "1942-12-07-00-00", "s3n://basekb-now/");
        List<FlowStep> steps=basekbNowFlow().generateSteps(flowArgs);
        Assert.assertNotNull(steps);
        assertEquals(5,steps.size());


        Map<String, Object> local = Maps.newHashMap();
        int j=0;
        {
            Assert.assertTrue(steps.get(j) instanceof AssignmentStep);
            AssignmentStep that=(AssignmentStep) steps.get(j++);
            local=that.process(local, flowArgs);
        }

        {
            Assert.assertTrue(steps.get(j) instanceof AssignmentStep);
            AssignmentStep that=(AssignmentStep) steps.get(j++);
            local=that.process(local, flowArgs);
        }

        {
            Assert.assertTrue(steps.get(j) instanceof SpringStep);
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
            Assert.assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step1=(SpringStep) steps.get(j++);
            List<String> args=step1.getStepArgs(local,flowArgs);

            assertEquals(6,args.size());

            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("pse3",args.get(i++));
            assertEquals("-r",args.get(i++));
            assertEquals("210",args.get(i++));
            assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
            assertEquals("s3n://basekb-now/1942-12-07-00-00/",args.get(i++));
        }

        {
            Assert.assertTrue(steps.get(j) instanceof SpringStep);
            SpringStep step2=(SpringStep) steps.get(j++);
            List<String> args=step2.getStepArgs(local,flowArgs);

            assertEquals(4,args.size());

            int i=0;
            assertEquals("run",args.get(i++));
            assertEquals("fs",args.get(i++));
            assertEquals("-rmr",args.get(i++));
            assertEquals("/preprocessed/1942-12-07-00-00/",args.get(i++));
        }
    }

    @Test
    public void checkCorrectConfigurationForBaseKBNowFlow() throws IOException {
        List<String> flowArgs=Lists.newArrayList("s3n://freebase-dumps/","1942-12-07-00-00","s3n://basekb-now/");
        List<StepConfig> steps=tinyAwsCluster.createEmrSteps(
                basekbNowFlow(),
                flowArgs,
                null
        );
        assertEquals(4,steps.size());
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
                            , "freebaseRDFPrefilter"
                            , "s3n://freebase-dumps/freebase-rdf-1942-12-07-00-00/"
                            , "/preprocessed/1942-12-07-00-00/")
                    ,that.getArgs());
        }

        {
            StepConfig c=steps.get(j++);
            HadoopJarStepConfig that=c.getHadoopJarStep();
            assertEquals(
                    Arrays.asList(
                            "run"
                            ,"pse3"
                            ,"-r"
                            ,"210"
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
                            ,"fs"
                            ,"-rmr"
                            ,"/preprocessed/1942-12-07-00-00/")
                    ,that.getArgs());
        }
    }

}
