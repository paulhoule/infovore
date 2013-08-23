package com.ontology2.haruhi;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.ontology2.centipede.shell.ExternalProcessFailedWithErrorCode;

public class LocalCmdCluster implements Cluster {
    private static Log logger = LogFactory.getLog(LocalCmdCluster.class);
    
    @Override
    public void runJob(String clusterId, String jarName, String[] jarArgs) throws Exception {
        
        List<String> args=Lists.newArrayList("/bin/bash","hadoop","jar",jarName);
        for(String arg:jarArgs) {
            args.add(arg);
        }
        
        ProcessBuilder pb = new ProcessBuilder(args);

        Process p = pb.start();
        int value=p.waitFor();
        if(value!=0) {
            throw new ExternalProcessFailedWithErrorCode(value);
        };
    }
};
