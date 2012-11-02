package com.ontology2.hydroxide;

import com.google.common.base.Predicate;

public class MatchesProperty implements Predicate<FreebaseQuad> {
	private final String property;
	
	public MatchesProperty(String property) {
		super();
		this.property = property;
	}

	@Override
	public boolean apply(FreebaseQuad q) {
		return property.equals(q.getProperty());
	}

}
