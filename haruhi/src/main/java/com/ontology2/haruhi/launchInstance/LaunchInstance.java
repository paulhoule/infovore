package com.ontology2.haruhi.launchInstance;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("launchInstance")
public class LaunchInstance extends CommandLineApplication {
    @Autowired
    AmazonEC2Client ec2Client;

    @Override
    protected void _run(String[] strings) throws Exception {
        RunInstancesRequest rir = getTinyRunInstancesRequest();

        RunInstancesResult response=ec2Client.runInstances(rir);
        Reservation r=response.getReservation();
        List<Instance> instances=r.getInstances();
        String instanceId=instances.get(0).getInstanceId();

        while(instanceStarting(instanceId)) {
            Thread.sleep(10*1000);
            System.out.println("testing status");
        }

        String ipAddress=findIpAddress(instanceId);
        System.out.println("instance "+instanceId+" is up and running at IP address: "+ipAddress);

    }

    private boolean instanceStarting(String instanceId) {
        DescribeInstancesResult result=ec2Client.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceId));
        return result.getReservations().get(0).getInstances().get(0).getState().getName().equals(InstanceStateName.Pending.toString());
    };

    private String findIpAddress(String instanceId) {
        DescribeInstancesResult result=ec2Client.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceId));
        return result.getReservations().get(0).getInstances().get(0).getPublicIpAddress();
    };

    private RunInstancesRequest getTinyRunInstancesRequest() {
        String theAmi="ami-018c9568";   // Stock Ubuntu 14.04 LTS for now
        return new RunInstancesRequest(theAmi,1,1)
                .withInstanceType("t1.micro")
                .withKeyName("o2key")
                .withSecurityGroups("launch-wizard-21")
                .withMonitoring(true);
    }

    private RunInstancesRequest getBigRunInstancesRequest() {
        String theAmi="ami-b68660de";
        return new RunInstancesRequest(theAmi,1,1)
                .withInstanceType("r3.xlarge")
                .withKeyName("o2key")
                .withSecurityGroups("launch-wizard-21")
                .withMonitoring(true);
    }
}
