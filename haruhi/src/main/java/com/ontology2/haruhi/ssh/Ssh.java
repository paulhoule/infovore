package com.ontology2.haruhi.ssh;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.ontology2.centipede.shell.CommandLineApplication;
import com.ontology2.centipede.errors.UsageException;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;

import java.util.List;

import static java.lang.System.*;

public class Ssh extends CommandLineApplication {
    @Resource
    AmazonEC2Client ec2Client;

    @Resource
    AmazonElasticMapReduceClient emrClient;

    @Resource
    private String clusterUsername;

    @Resource
    private String dotHaruhi;

    @Resource
    private String keyPairName;

    @Override
    protected void _run(String[] strings) throws Exception {
        if(strings.length<1)
            throw new UsageException("must specify AWS instance number");

        String targetId=strings[0];
        String instanceId="";

        if(targetId.startsWith("j-")) {
            instanceId=getClusterHeadInstanceId(targetId);
        }

        if(targetId.startsWith("i-")) {
            instanceId=targetId;
        }

        if(instanceId=="")
            throw new Exception("Could not find an instance id to log into");

        logIntoInstance(instanceId);
    }


    private String getClusterHeadInstanceId(String jobFlowId) throws Exception {
        DescribeInstancesResult r=ec2Client.describeInstances(
            new DescribeInstancesRequest().withFilters(
                new Filter().withName("tag:aws:elasticmapreduce:instance-group-role").withValues("MASTER"),
                new Filter().withName("tag:aws:elasticmapreduce:job-flow-id").withValues(jobFlowId)
            )
        );

        List<Reservation> instances=r.getReservations();
        if(instances.size() != 1)
            throw new Exception("Could not find cluster head ["+jobFlowId+"]");

        return instances.get(0).getInstances().get(0).getInstanceId();
    }

    private void logIntoInstance(String instanceId) {
    }
}
