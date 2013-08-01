package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.ontology2.millipede.sink.GroupingSink;

public abstract class NQuadsGroupOnSubject extends GroupingSink<Quad> {
    @Override
    protected Node computeGroupKey(Quad q) {
        return q.getSubject();
    }
}
