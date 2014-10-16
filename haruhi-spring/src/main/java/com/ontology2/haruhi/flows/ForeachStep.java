package com.ontology2.haruhi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class ForeachStep extends FlowStep {
    private final List<FlowStep> flowSteps;
    private final String loopVar;
    private final List<Object> values;

    public ForeachStep(String loopVar,List<Object> values, List<FlowStep> flowSteps) {
        this.loopVar=loopVar;
        this.values=values;
        this.flowSteps=flowSteps;
    }

    public List<FlowStep> getFlowSteps() {
        return flowSteps;
    }

    public String getLoopVar() {
        return loopVar;
    }

    public List<Object> getValues() {
        return values;
    }
}
