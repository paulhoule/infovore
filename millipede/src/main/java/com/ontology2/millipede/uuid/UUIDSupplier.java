package com.ontology2.millipede.uuid;

import java.util.UUID;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.google.common.base.Supplier;

public class UUIDSupplier implements Supplier<Node> {
    @Override
    public Node get() {
        return Node.createURI("urn:uuid:"+UUID.randomUUID().toString());
    }

}
