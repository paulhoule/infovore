package com.ontology2.haruhi;

import static com.ontology2.centipede.shell.ExitCodeException.EX_SOFTWARE;
import static com.ontology2.centipede.shell.ExitCodeException.EX_UNAVAILABLE;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionStatusDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.simpleworkflow.model.ExecutionStatus;
import com.google.common.collect.Sets;
import com.ontology2.centipede.shell.ExitCodeException;
import com.ontology2.haruhi.flows.Flow;

public class AmazonEMRCluster implements Cluster {
    private static Log logger = LogFactory.getLog(AmazonEMRCluster.class);
    private final JobFlowInstancesConfig instances;
    
    @Autowired private String awsSoftwareBucket;
    @Autowired private AmazonElasticMapReduce emrClient;
    @Autowired private StepConfig debugStep;
    @Autowired private String awsLogUri;
    
    private final Set<String> doneStates=Sets.newHashSet("COMPLETED","FAILED","TERMINATED");
    
    public AmazonEMRCluster(JobFlowInstancesConfig instances) {
        this.instances = instances;
    }

    @Override
    public void runJob(MavenManagedJar defaultJar,List<String> jarArgs)
            throws Exception {
        String jarLocation=defaultJar.s3JarLocation(awsSoftwareBucket);
        StepConfig[] steps = {
                debugStep,
                new StepConfig("main",new HadoopJarStepConfig(jarLocation).withArgs(jarArgs))
        };
        
        RunJobFlowRequest that=new RunJobFlowRequest()
            .withName("Haruhi submitted job")
            .withSteps(steps)
            .withLogUri(awsLogUri)
            .withInstances(instances);
        RunJobFlowResult result=emrClient.runJobFlow(that);
        String jobFlowId=result.getJobFlowId();
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
            
            if(doneStates.contains(state)) {
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
            logger.info("AWS believes that "+jobFlowId+"successfully completed");
        } else if(state=="FAILED") {
            logger.error("AWS reports failure of "+jobFlowId+" ");
            throw ExitCodeException.create(EX_SOFTWARE);
        } else {
            logger.error("Process completed in state "+state+" not on done list");
            throw ExitCodeException.create(EX_SOFTWARE);
        }
    }

    @Override
    public void runFlow(MavenManagedJar defaultJar, Flow f,List<String> flowArgs) throws Exception {
        throw new Exception("runFlow() not implemented yet");
    }

}
