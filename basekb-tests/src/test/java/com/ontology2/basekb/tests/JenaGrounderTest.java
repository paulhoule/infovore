package com.ontology2.basekb.tests;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.ontology2.basekb.BaseIRI;
import com.ontology2.basekb.BasicIRIGrounder;
import com.ontology2.basekb.IRIGrounder;
import com.ontology2.basekb.NameResolutionStep;
import com.ontology2.basekb.jena.DefaultSpringConfiguration;
import com.ontology2.basekb.jena.RawConfiguration;
import com.ontology2.basekb.jena.JenaIRIGrounder;
import com.ontology2.basekb.jena.JenaNameStep;
import com.ontology2.basekb.jena.SparqlProtocol;

public class JenaGrounderTest extends BasicIRIGrounderTest {
	
	// inherits tests from BasicIRIGrounder so we know it resolves all of them correctly
	@Before
	
	public void setUp() {
		RawConfiguration jConfig=DefaultSpringConfiguration.getInstance().getRawConfiguration();
		grounder = new JenaIRIGrounder(jConfig);	
	};
	
	@Test
	public void testRedirect() {
		assertLookup("m.01hfhqw","m.0bf4yyj");
	};
	
	@Test
	public void testNoRedirect() {	
	}
	
}
