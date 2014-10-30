package com.ontology2.haruhi;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.ontology2.centipede.errors.ExitCodeException.*;

import java.util.*;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.google.common.base.Joiner;
import com.ontology2.haruhi.alert.AlertService;
import com.ontology2.haruhi.emr.NodeType;
import com.ontology2.haruhi.fetchLogs.FetchLogs;
import com.ontology2.haruhi.flows.*;
import com.ontology2.haruhi.ssh.HadoopConfigurationVariable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ontology2.centipede.errors.ExitCodeException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

public class AmazonEMRCluster implements Cluster {
    private static Log logger = LogFactory.getLog(AmazonEMRCluster.class);
    private final JobFlowInstancesConfig instances;

    @Autowired private AmazonEC2Client ec2Client;
    @Autowired private String awsSoftwareBucket;
    @Autowired private AmazonElasticMapReduce emrClient;
    @Autowired private StepConfig debugStep;
    @Autowired private String awsLogUri;
    
    private final Set<String> doneStates=Sets.newHashSet("COMPLETED","FAILED","TERMINATED");
    
    public AmazonEMRCluster(JobFlowInstancesConfig instances) {
        this.instances = instances;
    }

    @PostConstruct
    public void init() {
        if(this.keyPairName!=null && !this.keyPairName.isEmpty())
            instances.setEc2KeyName(keyPairName);
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
            .withBootstrapActions(bootstrapActions())
            .withSteps(steps)
            .withLogUri(awsLogUri)
            .withInstances(instances);

        RunJobFlowResult result = runJob(that);

        pollClusterForCompletion(result);
        fetchLogs.run(new String[] {result.getJobFlowId()});
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
        boolean tagged=false;
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

            if(state.equals("RUNNING") && !tagged) {
                tagInstancesForJob(jobFlowId);
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

    private void tagInstancesForJob(String jobFlowId) {
        DescribeInstancesResult r=ec2Client.describeInstances(
                new DescribeInstancesRequest().withFilters(
                        new Filter().withName("tag:aws:elasticmapreduce:job-flow-id").withValues(jobFlowId)
                )
        );

        List<String> clusterInstances=newArrayList();
        for(Reservation that:r.getReservations())  {
            clusterInstances.add(that.getInstances().get(0).getInstanceId());
        }

        ec2Client.monitorInstances(new MonitorInstancesRequest(clusterInstances));
    };

    @Autowired private FetchLogs fetchLogs;
    @Autowired private AlertService alertService;

    @Override
    public void runFlow(MavenManagedJar jar, Flow f,List<String> flowArgs) throws Exception {
        String jarLocation=jar.s3JarLocation(awsSoftwareBucket);
        List<StepConfig> steps = createEmrSteps(f, flowArgs, jarLocation);

        String jobName = computeJobName(flowArgs);
        RunJobFlowRequest that=new RunJobFlowRequest()
        .withName(jobName)
        .withBootstrapActions(bootstrapActions())
        .withSteps(steps)
        .withLogUri(awsLogUri)
        .withInstances(instances);

        RunJobFlowResult result = runJob(that);
        String jobFlowId=result.getJobFlowId();
        pollClusterForCompletion(result);
        fetchLogs.run(new String[] {jobFlowId});
        alertService.alert("Cluster execution complete:\n"+jobName);

    }

    private Collection<BootstrapActionConfig> bootstrapActions() throws Exception {
        Map<HadoopConfigurationVariable, String> params = getHadoopConfigurationVariableStringMap();
        if(params.isEmpty()) {
            return newArrayList();
        }

        BootstrapActions.ConfigureHadoop a=
                new BootstrapActions().newConfigureHadoop();
        for(Map.Entry<HadoopConfigurationVariable,String> that:params.entrySet())
            a.withKeyValue(
                    that.getKey().getConfigFile(),
                    that.getKey().getKey(),
                    that.getValue());

        return newArrayList(
            a.build()
        );

    }

    @Resource
    private Map<String,NodeType> awsInstanceMap;
    @Resource
    private String keyPairName;

    Map<HadoopConfigurationVariable, String> getHadoopConfigurationVariableStringMap() throws Exception {
        Map<HadoopConfigurationVariable,String> out=newHashMap();

        String instanceType=instances.getSlaveInstanceType();

        if(!awsInstanceMap.containsKey(instanceType)) {
            logger.warn("I don't have hadoop configuration settings for ["+instanceType+"] using AWS defaults");
            return out;
        }

        Map<String,String> that=awsInstanceMap.get(instanceType).getHadoopParameters();
        for(Map.Entry<String,String> item:that.entrySet())
            out.put(new HadoopConfigurationVariable(item.getKey()),item.getValue());

        return out;
    }

    List<StepConfig> createEmrSteps(
            Flow f,
            List<String> flowArgs,
            String jarLocation) {
        List<StepConfig> steps= newArrayList(debugStep);
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
        List<StepConfig> steps= newArrayList();
        Map<String,Object> local= newHashMap(upperScopeVariables);
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
        alertService.alert("Cluster execution starting:\n for job"+that.getName()+" with jon flow Id"+result.getJobFlowId());
        logger.info("got job flow id "+result.getJobFlowId());

        alertService.alert("Cluster execution ending:\n for job"+that.getName()+" with job flow Id"+result.getJobFlowId());

//        emrClient.addTags(new AddTagsRequest(result.getJobFlowId(), Lists.newArrayList(
//                new Tag("com.ontology2.jobFlowId", result.getJobFlowId())
//        )));
//        logger.info("tags added to job flow ");
        return result;
    }
}
