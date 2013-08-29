package com.ontology2.haruhi;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class AmazonEMRCluster implements Cluster {
    private final JobFlowInstancesConfig instances;
    
    @Autowired private String awsSoftwareBucket;
    @Autowired private AmazonElasticMapReduce emrClient;
    @Autowired private StepConfig debugStep;
    @Autowired private String awsLogUri;
    
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
            .withLogUri(awsLogUri);
        
    }

}
