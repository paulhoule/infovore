package com.ontology2.haruhi;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.ontology2.centipede.shell.CentipedeShell;
import com.ontology2.centipede.shell.CommandLineApplication;

public class JobApp extends CommandLineApplication {
    private static Log logger = LogFactory.getLog(JobApp.class);
    
    @Autowired private ApplicationContext applicationContext;
    @Autowired private MavenManagedJar defaultJar;
    @Autowired private Cluster defaultCluster;
    
    @Override
    protected void _run(String[] arguments) throws Exception {
        
        PeekingIterator<String> a=Iterators.peekingIterator(Iterators.forArray(arguments));
        if (!a.hasNext())
            usage();

        Cluster cluster=defaultCluster;
        MavenManagedJar jar=defaultJar;
        while(a.hasNext() && a.peek().startsWith("-")) {
            String flagName=a.next().substring(1).intern();
            if (!a.hasNext())
                usage();
            
            String flagValue=a.next();
            if (flagName=="clusterId") {
                cluster=applicationContext.getBean(flagValue,Cluster.class);
            } else if(flagName=="jarId") {
                jar=applicationContext.getBean(flagValue,MavenManagedJar.class);
            } else {
                usage();
            };
        }
        List<String> jarArgs=defaultJar.getHeadArguments(); 
        Iterators.addAll(jarArgs, a);
        
        cluster.runJob(defaultJar,jarArgs);
    }

    private void usage() {
        System.out.println("To submit a job to the JobApplication do the following:");
        System.out.println();
        System.out.println("haruhi run job [options] jar_args ...");
        System.out.println("");
        System.out.println("The system will pass on any arguments beyond the options to ");
        System.out.println("the Hadoop application.  The system will use default options for the cluster");
        System.out.println("and JAR configuration unless you override them with the following options:");
        System.out.println("");
        System.out.println(" -clusterId <clusterId>");
        System.out.println(" -jarId <jarId>");
        System.out.println("");
        System.out.println("both of these arguments of Spring bean names.  If you want to add new");
        System.out.println("configurations,  this application searches");
        System.out.println("");
        System.out.println("$HOME/.haruhi/applicationContext.xml");
        System.out.println("");
        System.out.println("where you can override existing bean definitions or define new ones.");
        System.exit(-1);
    }

}
