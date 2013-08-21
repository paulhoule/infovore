package com.ontology2.haruhi;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

public class LocalCmdCluster implements Cluster {
    private static Log logger = LogFactory.getLog(LocalCmdCluster.class);
    
    @Override
    public void runJob(String clusterId, String jarName, String[] jarArgs) throws Exception {
        
        List<String> args=Lists.newArrayList("hadoop","jar",jarName);
        for(String arg:jarArgs) {
            args.add(arg);
        }
        
        ProcessBuilder pb = new ProcessBuilder(args);

        Process p = pb.start();
        int value=p.waitFor();
        if(value!=0) {
            logger.fatal("Hadoop process retuned with error value: "+value);
            System.exit(value);
        };
    }
};
