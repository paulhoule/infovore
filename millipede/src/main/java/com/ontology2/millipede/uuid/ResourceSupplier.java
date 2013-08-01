package com.ontology2.millipede.uuid;

import com.google.common.base.Supplier;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

public class ResourceSupplier implements Supplier<Resource> {

    private final Supplier<Node> innerSupplier;
    private final Model m;

    public ResourceSupplier(Model m,Supplier<Node> innerSupplier) {
        this.m=m;
        this.innerSupplier=innerSupplier;
    }

    @Override
    public Resource get() {
        return m.wrapAsResource(innerSupplier.get());
    }

}
