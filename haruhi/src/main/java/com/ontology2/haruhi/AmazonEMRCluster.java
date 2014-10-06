package com.ontology2.haruhi;

import static com.ontology2.centipede.errors.ExitCodeException.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.elasticmapreduce.model.*;
import com.google.common.base.Joiner;
import com.ontology2.haruhi.flows.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ontology2.centipede.errors.ExitCodeException;

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

    public String createPersistentCluster(String clusterName) throws Exception {
        StepConfig[] steps = {
                debugStep
        };

        instances.setKeepJobFlowAliveWhenNoSteps(true);
        RunJobFlowRequest that=new RunJobFlowRequest()
                .withName(clusterName)
                .withSteps(steps)
                .withLogUri(awsLogUri)
                .withInstances(instances);

        RunJobFlowResult result = runJob(that);

        pollClusterForCompletion(result, Sets.union(doneStates, Sets.newHashSet("WAITING")));
        return result.getJobFlowId();
    }



    @Override
    public void runJob(MavenManagedJar defaultJar,List<String> jarArgs)
            throws Exception {
        String jarLocation=defaultJar.s3JarLocation(awsSoftwareBucket);
        StepConfig[] steps = {
                debugStep,
                new StepConfig("main",new HadoopJarStepConfig(jarLocation).withArgs(jarArgs))
        };

        String jobName = computeJobName(jarArgs);

        RunJobFlowRequest that=new RunJobFlowRequest()
            .withName(jobName)
            .withSteps(steps)
            .withLogUri(awsLogUri)
            .withInstances(instances);

        RunJobFlowResult result = runJob(that);

        pollClusterForCompletion(result);
    }

    private void pollClusterForCompletion(RunJobFlowResult result) throws Exception {
        pollClusterForCompletion(result,doneStates);
    }

    String computeJobName(List<String> jarArgs) {
        String jobName = Joiner.on(" ").join(jarArgs);
        if (jobName.length()>255) {
            jobName=jobName.substring(0,255);
        }
        return jobName;
    }

    private void pollClusterForCompletion(RunJobFlowResult result,Set<String> completionStates)
            throws Exception {
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
    public void runFlow(MavenManagedJar jar, Flow f,List<String> flowArgs) throws Exception {
        String jarLocation=jar.s3JarLocation(awsSoftwareBucket);
        List<StepConfig> steps = createEmrSteps(f, flowArgs, jarLocation);

        String jobName = computeJobName(flowArgs);
        RunJobFlowRequest that=new RunJobFlowRequest()
        .withName(jobName)
        .withSteps(steps)
        .withLogUri(awsLogUri)
        .withInstances(instances);

        RunJobFlowResult result = runJob(that);
        pollClusterForCompletion(result);
    }

    List<StepConfig> createEmrSteps(
            Flow f,
            List<String> flowArgs,
            String jarLocation) {
        List<StepConfig> steps=Lists.newArrayList(debugStep);
        steps.addAll(createEmrSteps(
                f.generateSteps(flowArgs),
                flowArgs,
                jarLocation,
                new HashMap<String,Object>()
        ));
        return steps;
    }
    List<StepConfig> createEmrSteps(
            List<FlowStep> innerSteps,
            List<String> flowArgs,
            String jarLocation,
            Map<String,Object> upperScopeVariables
    ) {
        List<StepConfig> steps=Lists.newArrayList();
        Map<String,Object> local=Maps.newHashMap(upperScopeVariables);
        for(FlowStep that:innerSteps)
            if(that instanceof JobStep) {
                JobStep j=(JobStep) that;
                steps.add(new StepConfig(
                        "main"
                        ,new HadoopJarStepConfig(jarLocation)
                            .withArgs(j.getStepArgs(local,flowArgs)))
                );
            } else if(that instanceof AssignmentStep) {
                AssignmentStep ass=(AssignmentStep) that;
                local = ass.process(local, flowArgs);
            } else if(that instanceof ForeachStep) {
                ForeachStep step=(ForeachStep) that;
                for(Object v:step.getValues()) {
                    local.put(step.getLoopVar(),v);
                    steps.addAll(createEmrSteps(step.getFlowSteps(),flowArgs,jarLocation,local));
                }

            } else{
                throw new RuntimeException("Could not process step of type "+that.getClass());
            }
        return steps;
    }

    RunJobFlowResult runJob(RunJobFlowRequest that) {
        logger.info("about to create job flow");
        RunJobFlowResult result=emrClient.runJobFlow(that);
        logger.info("got job flow id "+result.getJobFlowId());
//        emrClient.addTags(new AddTagsRequest(result.getJobFlowId(), Lists.newArrayList(
//                new Tag("com.ontology2.jobFlowId", result.getJobFlowId())
//        )));
        logger.info("tags added to job flow ");
        return result;
    }
}
