package com.ontology2.haruhi;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.ontology2.centipede.errors.ExitCodeException;
import com.ontology2.haruhi.flows.Flow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static com.ontology2.centipede.errors.ExitCodeException.EX_SOFTWARE;
import static com.ontology2.centipede.errors.ExitCodeException.EX_UNAVAILABLE;


public class PersistentCluster implements Cluster {
    private static Log logger = LogFactory.getLog(PersistentCluster.class);
    final String runningCluster;
    @Autowired String awsSoftwareBucket;
    @Autowired AmazonElasticMapReduce emrClient;
    int count=0;

    public PersistentCluster(String runningCluster) {
        this.runningCluster = runningCluster;
    }

    @Override
    public void runJob(MavenManagedJar defaultJar, List<String> jarArgs) throws Exception {
        String jarLocation=defaultJar.s3JarLocation(awsSoftwareBucket);
        count++;
        String stepName = UUID.randomUUID().toString();
        StepConfig step =
                new StepConfig(stepName,new HadoopJarStepConfig(jarLocation).withArgs(jarArgs));

        emrClient.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(runningCluster)
                .withSteps(step));

        Thread.sleep(5000); // Enough time to transition out of WAITING?
        pollClusterForCompletion(runningCluster, stepName);
    }

    //
    // XXX - copied code from AmazonEMRCluster will probably get pushed into a super class
    //

    private final Set<String> doneStates= Sets.newHashSet("COMPLETED", "FAILED", "TERMINATED");

    private void pollClusterForCompletion(String jobFlowId, final String stepName)
            throws Exception {
        logger.info("Polling job flow in AWS with id "+jobFlowId);
        StepExecutionStatusDetail stepStatus=null;
        String stepState=null;

        //
        // make it synchronous with polling
        //

        final long checkInterval=60*1000;
        String state="";  // always interned!
        while(true) {
            List<JobFlowDetail> flows=emrClient
                    .describeJobFlows(
                            new DescribeJobFlowsRequest()
                                    .withJobFlowIds(jobFlowId))
                    .getJobFlows();

            if(flows.isEmpty()) {
                logger.error("The job flow "+jobFlowId+" can't be seen in the AWS flow list");
                throw new Exception();
            }

            JobFlowDetail flow=flows.get(0);
            state=flow.getExecutionStatusDetail().getState().intern();

            if(doneStates.contains(state)) {
                logger.info("Job flow "+jobFlowId+" ended in state "+state);
                break;
            }

            logger.info("Job flow "+jobFlowId+" reported status "+state);
            Iterable<StepDetail> steps=flow.getSteps();
            StepDetail detail=getFirst(filter(steps, new Predicate<StepDetail>() {
                @Override
                public boolean apply(@Nullable StepDetail input) {
                    return input.getStepConfig().getName().equals(stepName);
                }
            }),null);

            if(detail==null) {
                logger.info("Step [" + stepName + "] has yet to start");
            } else {
                stepStatus=detail.getExecutionStatusDetail();
                stepState=stepStatus.getState().intern();
                if (stepState!="PENDING" && stepState!="RUNNING") {
                    break;
                }
            }
            Thread.sleep(checkInterval);
        }

        if(state=="") {
            logger.error("We never established communication with the cluster");
            throw ExitCodeException.create(EX_UNAVAILABLE);
        } else if(state=="COMPLETED") {
            logger.info("AWS believes that "+jobFlowId+" successfully completed");
        } else if(state=="WAITING") {
            logger.info("The cluster at "+jobFlowId+" is waiting for job steps");

            if (stepState=="COMPLETED") {
                logger.info("AWS believes that the step successfully completed");
            } else {
                logger.info("Step terminated in state "+stepState);
                throw ExitCodeException.create(EX_SOFTWARE);
            }

        } else if(state=="FAILED") {
            logger.error("AWS reports failure of "+jobFlowId+" ");
            throw ExitCodeException.create(EX_SOFTWARE);
        } else {
            logger.error("Process completed in state "+state+" not on done list");
            throw ExitCodeException.create(EX_SOFTWARE);
        }
    }

    @Override
    public void runFlow(MavenManagedJar defaultJar, Flow f, List<String> flowArgs) throws Exception {
        throw new NotImplementedException();
    }

}
