package com.ontology2.hydroxide.turtleZero;

import com.google.common.base.Function;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class ModelGrounder implements Function<Triple,Triple> {
	private final TurtleZero turtleZero;
	
	public ModelGrounder(TurtleZero turtleZero) {
		this.turtleZero=turtleZero;
	}

	public Triple apply(Triple arg0) {
		return arg0;
	}
	
}
