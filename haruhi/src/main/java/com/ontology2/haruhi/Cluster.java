package com.ontology2.haruhi;

import com.ontology2.haruhi.flows.Flow;

import java.util.List;

public interface Cluster {

    public void runJob(MavenManagedJar defaultJar, List<String> jarArgs) throws Exception;
    public void runFlow(MavenManagedJar defaultJar, Flow f,List<String> flowArgs) throws Exception;
}
