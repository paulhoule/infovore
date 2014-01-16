package com.ontology2.haruhi.killPersistentCluster;

import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import com.ontology2.centipede.parser.Required;

public class KillPersistentClusterOptions implements HasOptions {
        @Required
        @Option(description="AWS Id of running cluster",defaultValue="")
        public String runningCluster;
}
