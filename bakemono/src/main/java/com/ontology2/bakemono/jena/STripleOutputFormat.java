package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class STripleOutputFormat extends TripleOutputFormat<Node,NodePair> {

    @Override
    protected Triple makeTriple(Node key, NodePair value) {
        // TODO Auto-generated method stub
        return new Triple(key,value.getOne(),value.getTwo());
    }

}
