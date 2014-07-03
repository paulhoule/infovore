package com.ontology2.haruhi.createMachineImage;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;

public class CreateMachineImage extends CommandLineApplication {
    @Autowired
    AmazonEC2Client ec2Client;

    @Override
    protected void _run(String[] strings) throws Exception {
        String instanceId="i-3c9b476f";
        String name="Testing "+System.currentTimeMillis();
        DescribeInstancesRequest dir= new DescribeInstancesRequest().withInstanceIds(instanceId);
        DescribeInstancesResult dir2=ec2Client.describeInstances(dir);
        Reservation rs=dir2.getReservations().get(0);
        Instance i=rs.getInstances().get(0);
        String instanceState=i.getState().getName().intern();

        if(instanceState!="stopped") {
            System.out.println("You must stop this instance safely before I can image it.");
            System.exit(-1);
        }

        CreateImageRequest request=new CreateImageRequest(instanceId,name).withBlockDeviceMappings(
                new BlockDeviceMapping().withVirtualName("ephemeral0").withDeviceName("/dev/xvdb"),
                new BlockDeviceMapping().withDeviceName("/dev/sdf").withEbs(new EbsBlockDevice().withDeleteOnTermination(true))
        );
        CreateImageResult result=ec2Client.createImage(request);
        System.out.println(result.getImageId());
    }
}
