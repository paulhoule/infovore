package com.ontology2.haruhi.newPersistentCluster;

import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import com.ontology2.centipede.parser.Required;

public class NewPersistentClusterOptions implements HasOptions {
    @Required
    @Option(description="Java Bean Name for cluster driver",defaultValue="")
    public String clusterId;
}
