package com.ontology2.haruhi;

import java.io.IOException;
import java.util.List;

public interface Cluster {

    public void runJob(MavenManagedJar defaultJar, List<String> jarArgs) throws Exception;

}
