package com.ontology2.basekb.tests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ontology2.basekb.NameResolutionStep;
import com.ontology2.basekb.jena.AnyQueryExecutionFactory;
import com.ontology2.basekb.jena.DefaultSpringConfiguration;
import com.ontology2.basekb.jena.RawConfiguration;
import com.ontology2.basekb.jena.JenaNameStep;
import com.ontology2.basekb.jena.SparqlProtocol;


import static org.junit.Assert.*;

public class NameStepTest {
	private NameResolutionStep step;
	
	@Before
	public void setUp() {
		RawConfiguration jConfig=DefaultSpringConfiguration.getInstance().getRawConfiguration();
		step = new JenaNameStep(jConfig);
	}
	
	@After
	public void tearDown() {
	}
	
	@Test
	public void lookupRootNamespace() {
		String rootNs=step.lookup("http://rdf.basekb.com/ns/m.03","root_namespace");
		assertEquals("http://rdf.basekb.com/ns/m.01",rootNs);
	}
	
	@Test
	public void lookupJockTaylor() {
		String jockMid=step.lookup("http://rdf.basekb.com/ns/m.02t9t5k","jock_taylor");
		assertEquals("http://rdf.basekb.com/ns/m.043_z",jockMid);
	}
	
	@Test
	public void lookupEn() {
		String enMid=step.lookup("http://rdf.basekb.com/ns/m.01","en");
		assertEquals("http://rdf.basekb.com/ns/m.02t9t5k",enMid);
	}
	
}
