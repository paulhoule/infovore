package com.ontology2.hydroxide;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.sink.GroupingSink;

abstract public class GroupOnSubject extends GroupingSink<Triple> {
	@Override
	protected Node computeGroupKey(Triple t) {
		return t.getSubject();
	}
}
