package com.ontology2.millipede.triples;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class Triples {

	public static Ordering<Triple> comparator() {
		return Ordering.usingToString().onResultOf(projectSubject())
		.compound(Ordering.usingToString().onResultOf(projectPredicate()))
		.compound(Ordering.usingToString().onResultOf(projectObject()));
	}

	private static Function<Triple, Node> projectObject() {
		return new Function<Triple,Node>() {
			@Override @Nullable
			public Node apply(@Nullable Triple arg0) {
				return arg0.getObject();
			}		
		};
	}

	private static Function<Triple, Node> projectPredicate() {
		return new Function<Triple,Node>() {
			@Override @Nullable
			public Node apply(@Nullable Triple arg0) {
				return arg0.getPredicate();
			}		
		};
	}

	private static Function<Triple, Node> projectSubject() {
		return new Function<Triple,Node>() {
			@Override @Nullable
			public Node apply(@Nullable Triple arg0) {
				return arg0.getSubject();
			}
		};
	};

}
