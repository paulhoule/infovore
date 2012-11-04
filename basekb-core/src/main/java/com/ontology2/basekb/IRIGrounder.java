package com.ontology2.basekb;

public interface IRIGrounder {

	/**
	 * @param name IRI of resource,  typically in baseKB RDF namespace
	 * @return unique mid identifier in BaseKB
	 */
	
	public String lookup(String name);
}
