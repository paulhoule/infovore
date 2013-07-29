package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Node;

public class NodePair {
	final Node one;
	final Node two;
	
	public NodePair(Node one,Node two) {
		this.one=one;
		this.two=two;
	};
	
	public Node getOne() {
		return one;
	}
	
	public Node getTwo() {
		return two;
	}
}
