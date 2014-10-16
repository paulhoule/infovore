package com.ontology2.haruhi.flows;

import java.util.List;

//
// at this point the base FlowStep doesn't do anything because different
// Clusters may do radically different things with different step types;
// for instance,  a local cluster will run all flows sequentally while
// an EMR cluster will batch them together,  then run them
//

public abstract class FlowStep {
}
