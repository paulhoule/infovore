package com.ontology2.rdf;

import com.hp.hpl.jena.graph.Node;

public class BKBInternal {
    public final static String baseURI="http://rdf.basekb.com/internal/";

    public final static Node SchemaObject=Node.createURI(baseURI+"SchemaObject");
    public final static Node SuppressedProperty=Node.createURI(baseURI+"SuppressedProperty");

    public final static Node knownAs=Node.createURI(baseURI+"knownAs");
    public final static Node hasLangTag=Node.createURI(baseURI+"hasLangTag");
    public final static Node hasTypeTag=Node.createURI(baseURI+"hasTypeTag");
    public final static Node uses=Node.createURI(baseURI+"uses");
    public final static Node rewrittenAs=Node.createURI(baseURI+"rewrittenAs");

    public final static Node Namespace=Node.createURI(baseURI+"Namespace");
}
