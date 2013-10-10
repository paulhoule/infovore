package com.ontology2.haruhi.flows;

import java.util.Arrays;
import java.util.List;

//
// This is inspired by the "JobFlow" concept in Amazon EMR;  the difference is
// that this is compatible with both local Hadoop and EMR.
//
// the key thing about this is that it transforms the arguments (which could
// be paths) into the arguments of a number of job steps which are run
// sequentially against a local cluster or submitted together for a batch
//

public abstract class Flow {
    abstract public List<FlowStep> generateSteps(List<String> flowArgs);
    
    public List<FlowStep> generateSteps(String... flowArgs) {
        return generateSteps(Arrays.asList(flowArgs));
    }
}
