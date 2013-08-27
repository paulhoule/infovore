package com.ontology2.haruhi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.ontology2.centipede.shell.ExternalProcessFailedWithErrorCode;

public class LocalCmdCluster implements Cluster {
    private static Log logger = LogFactory.getLog(LocalCmdCluster.class);
    
    //
    // For better or worse,  this version has a property that other clusters may not have,
    // in that it will print the stderr and stdout of the hadoop process to standard out
    //
    
    @Override
    public void runJob(String jarName, String[] jarArgs) throws Exception {
        String hadoopBin=findBin("hadoop");
        if(hadoopBin==null) {
            throw new IOException("Hadoop Executable not found");
        }
        
        List<String> args=Lists.newArrayList(hadoopBin,"jar",jarName);
        for(String arg:jarArgs) {
            args.add(arg);
        }
        
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        pb.redirectOutput(Redirect.INHERIT);
        Process p = pb.start();
       
        int value=p.waitFor();
        if(value!=0) {
            throw new ExternalProcessFailedWithErrorCode(value);
        };
    }

    private String findBin(String cmd) {
        String path=System.getenv("PATH");
        String fileSeparator=System.getProperty("file.separator");
        String pathSeparator=System.getProperty("path.separator");
        for(String dir:Splitter.on(pathSeparator).split(path)) {
            File target=new File(dir,cmd);
            if(target.canExecute()) {
                return target.getAbsolutePath();
            }
        }
        return null;
    }
};
