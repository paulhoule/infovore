package com.ontology2.haruhi;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.google.common.collect.Sets;
import com.ontology2.centipede.shell.ExitCodeException;
import com.ontology2.haruhi.flows.Flow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Set;

import static com.ontology2.centipede.shell.ExitCodeException.EX_SOFTWARE;
import static com.ontology2.centipede.shell.ExitCodeException.EX_UNAVAILABLE;

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
        StepConfig step =
                new StepConfig("step "+count,new HadoopJarStepConfig(jarLocation).withArgs(jarArgs));

        emrClient.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(runningCluster)
                .withSteps(step));

        pollClusterForCompletion(runningCluster,Sets.union(doneStates,Sets.newHashSet("WAITING")));
    }

    //
    // XXX - copied code from AmazonEMRCluster will probably get pushed into a super class
    //

    private final Set<String> doneStates= Sets.newHashSet("COMPLETED", "FAILED", "TERMINATED");

    private void pollClusterForCompletion(String jobFlowId,Set<String> completionStates)
            throws Exception {
        logger.info("Created job flow in AWS with id "+jobFlowId);

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

            state=flows.get(0).getExecutionStatusDetail().getState().intern();

            if(completionStates.contains(state)) {
                logger.info("Job flow "+jobFlowId+" ended in state "+state);
                break;
            }

            logger.info("Job flow "+jobFlowId+" reported status "+state);
            Thread.sleep(checkInterval);
        }

        if(state=="") {
            logger.error("We never established communication with the cluster");
            throw ExitCodeException.create(EX_UNAVAILABLE);
        } else if(state=="COMPLETED") {
            logger.info("AWS believes that "+jobFlowId+" successfully completed");
        } else if(state=="WAITING") {
            logger.info("The cluster at "+jobFlowId+" is waiting for job steps");
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
