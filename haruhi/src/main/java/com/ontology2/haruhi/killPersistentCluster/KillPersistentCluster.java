package com.ontology2.haruhi.killPersistentCluster;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.google.common.collect.Lists;
import com.ontology2.centipede.parser.OptionParser;
import com.ontology2.centipede.shell.CommandLineApplication;
import com.ontology2.haruhi.Cluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

@Component("killPersistentCluster")
public class KillPersistentCluster extends CommandLineApplication {
    @Autowired private ApplicationContext applicationContext;
    @Autowired private Cluster defaultCluster;
    @Autowired private AmazonElasticMapReduce emrClient;

    @Override
    protected void _run(String[] arguments) throws Exception {
        KillPersistentClusterOptions options=extractOptions(arguments);
        emrClient.terminateJobFlows(
                new TerminateJobFlowsRequest()
                        .withJobFlowIds(options.runningCluster));
    }

    private KillPersistentClusterOptions extractOptions(String[] strings) throws IllegalAccessException {
        return extractOptions(Lists.newArrayList(strings));
    }
    private KillPersistentClusterOptions extractOptions(ArrayList<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(KillPersistentClusterOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        return (KillPersistentClusterOptions) parser.parse(strings);
    }
}
