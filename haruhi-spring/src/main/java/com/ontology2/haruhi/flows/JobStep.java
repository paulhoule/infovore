package com.ontology2.haruhi.flows;

import java.util.Arrays;
import java.util.List;


public class JobStep extends SpringStep {

    public JobStep(List<String> argDefinitions) {
        super(argDefinitions);
    }
    
    public JobStep(String... defs) { this(Arrays.asList(defs)); }

}
