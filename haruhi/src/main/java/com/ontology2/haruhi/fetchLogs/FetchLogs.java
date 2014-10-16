package com.ontology2.haruhi.fetchLogs;

import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;

import static com.google.common.base.Splitter.*;
import static com.google.common.collect.Iterables.*;

@Component
public class FetchLogs extends CommandLineApplication {
    @Autowired
    TransferManager transferManager;
    @Resource
    String localLogTarget;
    @Resource
    String awsLogUri;

    @Override
    protected void _run(String[] strings) throws Exception {
        for(String that:strings) {
            doItForJob(that);
        }

//        String instanceId="i-3c9b476f";
//        String name="Testing "+System.currentTimeMillis();
//        DescribeInstancesRequest dir= new DescribeInstancesRequest().withInstanceIds(instanceId);
//        DescribeInstancesResult dir2=ec2Client.describeInstances(dir);
//        Reservation rs=dir2.getReservations().get(0);
//        Instance i=rs.getInstances().get(0);
//        String instanceState=i.getState().getName().intern();
//
//        if(instanceState!="stopped") {
//            System.out.println("You must stop this instance safely before I can image it.");
//            System.exit(-1);
//        }
//
//        CreateImageRequest request=new CreateImageRequest(instanceId,name).withBlockDeviceMappings(
//                new BlockDeviceMapping().withVirtualName("ephemeral0").withDeviceName("/dev/xvdb"),
//                new BlockDeviceMapping().withDeviceName("/dev/sdf").withEbs(new EbsBlockDevice().withDeleteOnTermination(true))
//        );
//        CreateImageResult result=ec2Client.createImage(request);
//        System.out.println(result.getImageId());
    }

    private void doItForJob(String jobId) {
        String bucketName= getLast(on("/").omitEmptyStrings().split(awsLogUri));
        transferManager.downloadDirectory(bucketName, jobId, new File(localLogTarget));
    }
}
