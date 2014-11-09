package com.ontology2.haruhi;

import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.ontology2.haruhi.flows.Flow;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
        "classpath:com/ontology2/centipede/shell/applicationContext.xml",
        "shell/applicationContext.xml",
        "shell/testDefaults.xml"})
public class TestEMRCluster {
    @InjectMocks
    @Autowired AmazonEMRCluster tinyAwsCluster;
    @Mock
    private AmazonS3Client s3Client;

    @Autowired private StepConfig debugStep;
    @Autowired Flow foreachStepFlow;
    @Autowired Flow mostMonthsFlow;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

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

    @Test
    public void testMostMonthsFlow() {
        List<String> flowArgs=Lists.newArrayList();
        List<StepConfig> steps=tinyAwsCluster.createEmrSteps(
                mostMonthsFlow,
                flowArgs,
                null
        );

        assertEquals(49,steps.size());
    }

    @Test
    public void failsWithNoOptions() throws IllegalAccessException, URISyntaxException {
        assertFalse(tinyAwsCluster.validateJarArgs(new ArrayList<String>()));
    }

    @Test
    public void suceedsWithGoodPaths() throws IllegalAccessException, URISyntaxException {
        mockS3Client();

        assertTrue(tinyAwsCluster.validateJarArgs(new ArrayList<String>() {{
            add("-input");
            add("s3n://foo-bar/pathoDromic");
            add("-output");
            add("s3n://foo-bar/way-out/");
        }}));
    }

    @Test
    public void failsWithBadInput() throws IllegalAccessException, URISyntaxException {
        mockS3Client();

        assertFalse(tinyAwsCluster.validateJarArgs(new ArrayList<String>() {{
            add("-input");
            add("s3n://foo-bar/way-out/");
            add("-output");
            add("s3n://foo-bar/way-out/");
        }}));
    }

    @Test
    public void failsWithBadOutput() throws IllegalAccessException, URISyntaxException {
        mockS3Client();

        assertFalse(tinyAwsCluster.validateJarArgs(new ArrayList<String>() {{
            add("-input");
            add("s3n://foo-bar/pathoDromic");
            add("-output");
            add("s3n://foo-bar/pathoDromic");
        }}));
    }

    private void mockS3Client() {
        when(s3Client.listObjects(any(ListObjectsRequest.class))).thenAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args=invocationOnMock.getArguments();
                ListObjectsRequest r=(ListObjectsRequest) args[0];
                if(!"foo-bar".equals(r.getBucketName()))
                    throw new Exception("Bucket name was not correctly specified");
                if(!(1==r.getMaxKeys()))
                    throw new Exception("More than one result was asked for");

                boolean ok=false;
                final List<S3ObjectSummary> out= Lists.newArrayList();

                if("pathoDromic".equals(r.getPrefix())) {
                    out.add(new S3ObjectSummary());
                    ok=true;
                }

                if("way-out/".equals(r.getPrefix())) {
                    ok=true;
                }
                if(ok) {
                    return new ObjectListing() {
                        @Override
                        public List<S3ObjectSummary> getObjectSummaries() {
                            return out;
                        }

                    };
                }
                throw new Exception("An unrecognized path ["+r.getPrefix()+"] was given");
            }
        });
    }
}
