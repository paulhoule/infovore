package com.ontology2.haruhi.ssh;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.ontology2.centipede.shell.CommandLineApplication;
import com.ontology2.centipede.errors.UsageException;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;

import static java.lang.System.*;

public class Ssh extends CommandLineApplication {
    @Resource
    AmazonEC2Client ec2Client;

//    @Resource
//    private String clusterHeadUserName;

    @Resource
    private String dotHaruhi;

//    @Resource
//    private String keyPairName;

    @Override
    protected void _run(String[] strings) throws Exception {
        if(strings.length<1)
            throw new UsageException("must specify AWS instance number");

        String instanceId=strings[0];
        String username="ubuntu";   // how do we know this for sure?
        String keyFileLocation= getProperty("user.home")+"/.haruhi";

        DescribeInstancesResult r=ec2Client.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceId));
        Instance i=r.getReservations().get(0).getInstances().get(0);
        String ipAddress=i.getPublicIpAddress();
        String keyName=i.getKeyName();
        out.println("ssh -i \""+keyFileLocation+'/'+keyName+".pem\""+" "+username+"@"+ipAddress);
    }
}
