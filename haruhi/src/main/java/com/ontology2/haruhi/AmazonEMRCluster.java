package com.ontology2.haruhi;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;

public class AmazonEMRCluster implements Cluster {
    private final JobFlowInstancesConfig instances;
    
    @Autowired private String awsSoftwareBucket;
    @Autowired private AmazonElasticMapReduce emrClient;
    
    public AmazonEMRCluster(JobFlowInstancesConfig instances) {
        this.instances = instances;
    }

    @Override
    public void runJob(MavenManagedJar defaultJar,List<String> jarArgs)
            throws Exception {
        String jarLocation=defaultJar.s3JarLocation(awsSoftwareBucket);
    }

}
