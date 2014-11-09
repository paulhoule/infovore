package com.ontology2.haruhi.jobApp;

import com.google.common.collect.Lists;
import com.ontology2.centipede.parser.OptionParser;
import com.ontology2.centipede.shell.CommandLineApplication;
import com.ontology2.haruhi.Cluster;
import com.ontology2.haruhi.MavenManagedJar;
import com.ontology2.haruhi.PersistentCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component("job")
public class JobApp extends CommandLineApplication {
    private static Log logger = LogFactory.getLog(JobApp.class);
    
    @Autowired private ApplicationContext applicationContext;
    @Autowired private MavenManagedJar defaultJar;
    @Autowired private Cluster defaultCluster;
    
    @Override
    protected void _run(String[] arguments) throws Exception {
        JobAppOptions options=extractOptions(arguments);

        Cluster cluster=options.clusterId.isEmpty() ? defaultCluster : applicationContext.getBean(options.clusterId,Cluster.class);
        MavenManagedJar jar=options.jarId.isEmpty() ? defaultJar : applicationContext.getBean(options.jarId,MavenManagedJar.class);

        Map<String,String> env=System.getenv();

        if(options.runningCluster.isEmpty() && env.containsKey("RUNNING_CLUSTER")) {
            options.runningCluster=env.get("RUNNING_CLUSTER");
            logger.info("Getting AWS Job Flow ID ["+options.runningCluster+"] from RUNNING_CLUSTER environment variable");
        }

        if (!(options.runningCluster.isEmpty())) {
            cluster=new PersistentCluster(options.runningCluster);
            applicationContext.getAutowireCapableBeanFactory().autowireBean(cluster);
        }

        if(jar.getFirstArgumentIsNotAPath()) {
            if(options.remainingArguments.isEmpty())
                usage();

            String firstArgument=options.remainingArguments.get(0);

            if (firstArgument.contains(":") || firstArgument.contains("/"))
                usage();
        };
        List<String> jarArgs=jar.getHeadArguments();
        jarArgs.addAll(options.remainingArguments);
        cluster.runJob(jar,jarArgs);
    }

    private JobAppOptions extractOptions(String[] strings) throws IllegalAccessException {
        return extractOptions(Lists.newArrayList(strings));
    }
    private JobAppOptions extractOptions(ArrayList<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(JobAppOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        return (JobAppOptions) parser.parse(strings);
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
