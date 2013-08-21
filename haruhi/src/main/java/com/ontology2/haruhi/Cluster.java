package com.ontology2.haruhi;

import java.io.IOException;

public interface Cluster {

    public void runJob(String clusterId, String jarName, String[] jarArgs) throws Exception;

}
