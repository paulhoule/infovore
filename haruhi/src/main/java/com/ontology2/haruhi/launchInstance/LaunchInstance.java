package com.ontology2.haruhi.launchInstance;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component("launchInstance")
public class LaunchInstance extends CommandLineApplication {
    @Autowired
    AmazonEC2Client ec2Client;

    @Override
    protected void _run(String[] strings) throws Exception {
        RunInstancesRequest rir = getBigRunInstancesRequest();

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

        Thread.sleep(2*60*1000);   // primitive, ssh will take a little while to be ready

        final SSHClient ssh = new SSHClient();
        String $HOME=System.getProperty("user.home");
        OpenSSHKeyFile k=new OpenSSHKeyFile();
        k.init(new File($HOME+"/.haruhi/o2key.pem"));
        ssh.addHostKeyVerifier(new PromiscuousVerifier());

        ssh.connect(ipAddress);
        ssh.authPublickey("ubuntu",k);
        final Session session = ssh.startSession();
        try {
            final Session.Command cmd = session.exec("ls -a");

            System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
            cmd.join(5, TimeUnit.SECONDS);
            System.out.println("\n** exit status: " + cmd.getExitStatus());
        } finally {
            session.close();
            ssh.disconnect();
        }
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
        String theAmi="ami-fccd2d94";
        return new RunInstancesRequest(theAmi,1,1)
                .withInstanceType("r3.xlarge")
                .withKeyName("o2key")
                .withSecurityGroups("launch-wizard-21")
                .withMonitoring(true)
                .withPlacement(new Placement("us-east-1e"));
    }

    private RunInstancesRequest getEmptyVirtuosoInstancesRequest() {
        String theAmi="ami-f0a54298";
        return new RunInstancesRequest(theAmi,1,1)
                .withInstanceType("r3.large")
                .withKeyName("o2key")
                .withSecurityGroups("launch-wizard-21")
                .withMonitoring(true);
    }
}
