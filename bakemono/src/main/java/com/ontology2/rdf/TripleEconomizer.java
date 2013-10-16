package com.ontology2.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class TripleEconomizer implements Economizer<Triple> {

    private final Economizer<Node> innerEconomizer;

    public TripleEconomizer(Economizer<Node> innerEconomizer) {
        this.innerEconomizer=innerEconomizer;
    }

    public TripleEconomizer() {
        this(new CacheEconomizer<Node>());
    }

    private Node e(Node n) {
        return innerEconomizer.economize(n);
    }

    @Override
    public Triple economize(Triple that) {
        return new Triple(
                e(that.getSubject()),
                e(that.getPredicate()),
                e(that.getObject())
                );
    }


}
