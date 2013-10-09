package com.ontology2.haruhi.flows;

import java.util.List;

import com.ontology2.haruhi.MavenManagedJar;

/**
 * A SpringFlow processes creates a series of SpringSteps,  for each SpringStep
 * the system will parse the arguments with SPeL
 * 
 * I'd like to substitute the arguments of flowArgs into variables like
 * 
 * $0, $1, $2
 * 
 * and (in the future) pass in named arguments.
 *
 */

public class SpringFlow implements Flow {

    private final List<SpringStep> springSteps;
    
    public SpringFlow(List<SpringStep> springSteps) {
        this.springSteps = springSteps;
    }

    @Override
    public List<FlowStep> generateSteps(List<String> flowArgs) {
        return null;
    }
}
