package com.ontology2.haruhi;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.ontology2.centipede.shell.CentipedeShell;
import com.ontology2.centipede.shell.CommandLineApplication;

public class JobApplication extends CommandLineApplication {
    private static Log logger = LogFactory.getLog(JobApplication.class);
    
    @Autowired
    private ApplicationContext applicationContext; 
    
    @Override
    protected void _run(String[] arguments) throws Exception {
        
        //
        // stolen from centipede shell
        //
        
        if(arguments.length<2) {
            usage();
        }
        
        String clusterId=arguments[0];
        String jarName=arguments[1];
        String[] jarArgs= arguments.length<3 ? new String[0] 
                : Arrays.copyOfRange(arguments, 2, arguments.length);
        
        Cluster cluster=(Cluster) applicationContext.getBean(clusterId,Cluster.class);
        cluster.runJob(clusterId,jarName,jarArgs);
    }

    private void usage() {
        System.out.println("To submit a job to the JobApplication do the following:");
        System.out.println();
        System.out.println("haruhi job cluster_id jar_name jar_args ...");
        System.out.println();
        System.out.println("where the cluster id is the name of the cluster that we're sending the job to");
        System.out.println("and the jar_name is relative to the cluster configuration and other arguments");
        System.out.println("get supplied to the jar.");
    }

}
