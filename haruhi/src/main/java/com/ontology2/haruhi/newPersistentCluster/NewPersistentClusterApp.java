package com.ontology2.haruhi.newPersistentCluster;

import com.google.common.collect.Lists;
import com.ontology2.centipede.errors.UsageException;
import com.ontology2.centipede.parser.OptionParser;
import com.ontology2.centipede.shell.CommandLineApplication;
import com.ontology2.haruhi.AmazonEMRCluster;
import com.ontology2.haruhi.Cluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

@Component("newPersistentCluster")
public class NewPersistentClusterApp extends CommandLineApplication {
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired private Cluster defaultCluster;

    @Override
    protected void _run(String[] arguments) throws Exception {
        NewPersistentClusterOptions options=extractOptions(arguments);

        Cluster cluster=options.clusterId.isEmpty() ? defaultCluster : applicationContext.getBean(options.clusterId,Cluster.class);
        if (!(cluster instanceof AmazonEMRCluster)) {
            throw new UsageException("the -clusterId must specify an Amazon EMR Cluster");
        }

        String name=((AmazonEMRCluster) cluster).createPersistentCluster("Persistent Cluster");
        System.out.println(name);
    }

    private NewPersistentClusterOptions extractOptions(String[] strings) throws IllegalAccessException {
        return extractOptions(Lists.newArrayList(strings));
    }
    private NewPersistentClusterOptions extractOptions(ArrayList<String> strings) throws IllegalAccessException {
        OptionParser parser=new OptionParser(NewPersistentClusterOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        return (NewPersistentClusterOptions) parser.parse(strings);
    }
}
