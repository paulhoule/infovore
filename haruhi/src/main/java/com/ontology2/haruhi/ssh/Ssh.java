package com.ontology2.haruhi.ssh;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.ontology2.centipede.errors.UsageException;
import com.ontology2.centipede.shell.CommandLineApplication;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.StreamCopier;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class Ssh extends CommandLineApplication {
    private static Log logger = LogFactory.getLog(Ssh.class);
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

    private void logIntoInstance(String instanceId) throws Exception {
        logIntoIp(lookupIpOfInstance(instanceId));
    }

    private String lookupIpOfInstance(String instanceId) throws Exception {
        DescribeInstancesResult r=ec2Client.describeInstances(
                new DescribeInstancesRequest().withInstanceIds(instanceId)
        );

        List<Reservation> instances=r.getReservations();
        if(instances.size() != 1)
            throw new Exception("Could not find instance ["+instanceId+"]");

        return instances.get(0).getInstances().get(0).getPublicIpAddress();
    }

    private void logIntoIp(String ipAddress) throws IOException, InterruptedException {
        final SSHClient ssh = new SSHClient();
        OpenSSHKeyFile k = new OpenSSHKeyFile();
        k.init(new File(dotHaruhi, keyPairName + ".pem"));
        ssh.addHostKeyVerifier(new PromiscuousVerifier());

        ssh.connect(ipAddress);
        try {
            ssh.authPublickey(clusterUsername, k);
            Session that = ssh.startSession();
            try {
                that.allocateDefaultPTY();
                Session.Shell shell = that.startShell();

                new StreamCopier(shell.getInputStream(), System.out)
                        .bufSize(shell.getLocalMaxPacketSize())
                        .spawn("stdout");

                new StreamCopier(shell.getErrorStream(), System.err)
                        .bufSize(shell.getLocalMaxPacketSize())
                        .spawn("stderr");

                new StreamCopier(System.in, shell.getOutputStream())
                        .bufSize(shell.getRemoteMaxPacketSize())
                        .copy();

            } finally {
                that.close();
            }
        } finally {
            ssh.close();
        }
    }

    private Runnable redirect(final InputStream inputStream, final OutputStream outputStream) {
        return new Runnable() {

            @Override
            public void run() {
                logger.info("In copy thread "+Thread.currentThread());
                try {
                    while(true) {
                        int b=inputStream.read();
                        if (b==-1)
                            return;

                        outputStream.write((int) b);
                    }
                } catch(IOException ioEx) {

                } finally {
                    logger.info("Exiting copy thread " + Thread.currentThread());
                }
            }
        };
    }
}
