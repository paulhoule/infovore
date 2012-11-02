package com.ontology2.hydroxide;

import com.google.common.base.Predicate;

public class PropertyIs implements Predicate<FreebaseQuad> {

	private final String property;
	public PropertyIs(String property) {
		this.property=property.intern();
	}
	
	public boolean apply(FreebaseQuad arg0) {
		return (arg0.getProperty()==property);
	}
	
}
