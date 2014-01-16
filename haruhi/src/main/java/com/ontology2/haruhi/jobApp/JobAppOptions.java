package com.ontology2.haruhi.jobApp;

import com.ontology2.centipede.parser.HasOptions;
import com.ontology2.centipede.parser.Option;
import com.ontology2.centipede.parser.Positional;
import com.ontology2.centipede.parser.Required;

import java.util.List;

public class JobAppOptions implements HasOptions {
    @Option(description="Java Bean Name for cluster driver",defaultValue="")
    public String clusterId;

    @Required
    @Option(description="Java Bean Name for JAR coordinates",defaultValue="")
    public String jarId;

    @Option(description="Identifier for running cluster",defaultValue="")
    public String runningCluster;

    @Positional
    public List<String> remainingArguments;
}
