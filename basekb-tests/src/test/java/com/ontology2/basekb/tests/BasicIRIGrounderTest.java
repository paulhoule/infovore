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
import com.ontology2.basekb.jena.JenaNameStep;
import com.ontology2.basekb.jena.SparqlProtocol;

public class BasicIRIGrounderTest {
	
	protected IRIGrounder grounder;

	@Before
	public void setUp() {
		RawConfiguration jConfig=DefaultSpringConfiguration.getInstance().getRawConfiguration();
		grounder = new BasicIRIGrounder(new JenaNameStep(jConfig));
	};

	@Test
	public void testWater() {
		assertLookup("en.water","m.0838f");
	}
	
	@Test
	public void testBlacklisted() {
		assertLookup("boot.blacklisted","m.0c1jhh4");
	}
	
	@Test
	public void testLax() {
		assertLookup("authority.iata.LAX","m.04lyk");
	}
	
	@Test
	public void testBogus() {
		// name doesn't get transformed if we can't look it up
		assertLookup("bogus.phony.fake.namespace","bogus.phony.fake.namespace");
	}
	
	@Test
	public void passThroughMid() {
		assertLookup("m.03bg8n","m.03bg8n");
	}
	
	@Test
	public void testHasPermissions() {
		assertLookup("base.basekb.haspermissions","m.0j2r8kh");
	}
	
	@Test
	public void resolveAGuid() {
		assertLookup("guid.9202a8c04000641f80000000004170c1","m.042x61");
	}
	
	@Test
	public void resolveAGuidAgain() {
		assertLookup("guid.9202a8c04000641f8000000001f1558f","m.0_2pdh");
	}
	
	@Test
	public void resolveMusicbrainz() {
		assertLookup("authority.musicbrainz","m.0k8f1f");
	}
	@Test
	public void testNoRedirect() {
		assertLookup("m.01hfhqw","m.01hfhqw");		
	}
	
	@Test
	public void resolveAirport() {
		assertLookup("aviation.airport","m.01xpjyz");
	}
	
	protected void assertLookup(String key,String mid) {
		String output=grounder.lookup(BaseIRI.bkNs+key);
		String shouldBe=(mid==null) ? null : BaseIRI.bkNs+mid;
		assertEquals(shouldBe,output);		
	}

}
