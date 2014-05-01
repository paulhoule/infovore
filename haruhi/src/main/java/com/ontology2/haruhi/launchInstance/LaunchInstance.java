package com.ontology2.haruhi.launchInstance;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
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
        String theAmi="ami-b68660de";
        RunInstancesRequest rir=new RunInstancesRequest(theAmi,1,1)
                .withInstanceType("r3.xlarge")
                .withKeyName("o2key")
                .withSecurityGroups("launch-wizard-21")
                .withMonitoring(true);

        RunInstancesResult response=ec2Client.runInstances(rir);
        Reservation r=response.getReservation();
        List<Instance> instances=r.getInstances();
        for(Instance i:instances) {
            System.out.println(i.getInstanceId()+" "+i.getState());
        }
    }
}
