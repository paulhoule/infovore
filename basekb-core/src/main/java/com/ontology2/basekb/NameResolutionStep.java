package com.ontology2.basekb;

public interface NameResolutionStep {
	
	//
	// The namespace is a mid URI like
	//
	// http://rdf.basekb.com/ns/m.01
	//
	// and this method returns a similar mid URI.
	//
	
	public String lookup(String namespace,String name);
}
