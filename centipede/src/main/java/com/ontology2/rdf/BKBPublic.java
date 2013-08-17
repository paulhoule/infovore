package com.ontology2.rdf;

import com.hp.hpl.jena.graph.Node;

public class BKBPublic {
    public final static String baseURI="http://rdf.basekb.com/public/";
    public final static Node knownAs=Node.createURI(baseURI+"knownAs");
    public final static Node hasKeyProperty=Node.createURI(baseURI+"hasKeyProperty");
    public final static Node gravity=Node.createURI(baseURI+"gravity");
}
