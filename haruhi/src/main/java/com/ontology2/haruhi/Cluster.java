package com.ontology2.haruhi;

import java.io.IOException;
import java.util.List;

import com.ontology2.haruhi.flows.Flow;

public interface Cluster {

    public void runJob(MavenManagedJar defaultJar, List<String> jarArgs) throws Exception;
    public void runFlow(MavenManagedJar defaultJar, Flow f,List<String> flowArgs) throws Exception;
}
