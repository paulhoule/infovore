package com.ontology2.hydroxide;

import static org.junit.Assert.*;

import com.ontology2.hydroxide.ExpandFreebaseRdfToNTriples;
import static com.ontology2.hydroxide.ExpandFreebaseRdfToNTriples.splitPrefixDeclaration;
import static com.ontology2.hydroxide.ExpandFreebaseRdfToNTriples.splitTriple;

import java.util.List;

import org.junit.Test;

import com.ontology2.hydroxide.ExpandFreebaseRdfToNTriples.InvalidPrefixException;
import com.ontology2.millipede.sink.ListSink;
import com.ontology2.millipede.sink.NullSink;

public class ExpandFreebaseRdfToNTriplesTest {

	@Test
	public void testSplitPrefix() throws InvalidPrefixException {
		List<String> parts=splitPrefixDeclaration("@prefix foo: <http://bar.com/>.");
		assertEquals("foo",parts.get(1));
		assertEquals("http://bar.com/",parts.get(2));
	}

	@Test
	public void testSplitTriple() throws Exception {
		List<String> parts=splitTriple("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
		assertEquals(3,parts.size());
		assertEquals("ns:aviation.aircraft.first_flight",parts.get(0));
		assertEquals("ns:type.property.unique",parts.get(1));
		assertEquals("true",parts.get(2));
	}
	
	@Test
	public void testExpandTripleParts() throws Exception {
		ExpandFreebaseRdfToNTriples convert=createTestFixture();
		populateFreebasePrefixes(convert);
		List<String> parts=convert.expandTripleParts("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
		assertEquals(3,parts.size());
		assertEquals("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>",parts.get(0));
		assertEquals("<http://rdf.freebase.com/ns/type.property.unique>",parts.get(1));
		assertEquals("true",parts.get(2));
	}

	private ExpandFreebaseRdfToNTriples createTestFixture() {
		return new ExpandFreebaseRdfToNTriples(new NullSink<PrimitiveTriple>(),new NullSink<String>());
	}
	
	@Test
	public void testExpandNode() throws Exception {
		ExpandFreebaseRdfToNTriples convert=createTestFixture();
		populateFreebasePrefixes(convert);
		assertEquals("<http://www.w3.org/2000/01/rdf-schema#label>",
				convert.expandIRINode("rdfs:label"));
		assertEquals("<http://rdf.freebase.com/ns/type.object.type>",
				convert.expandIRINode("ns:type.object.type"));
		
	}
	
	@Test
	public void testExpandAnyNode() throws Exception {
		ExpandFreebaseRdfToNTriples convert=createTestFixture();
		populateFreebasePrefixes(convert);
		assertEquals("<http://www.w3.org/2000/01/rdf-schema#label>",
				convert.expandAnyNode("rdfs:label"));
		assertEquals("<http://rdf.freebase.com/ns/type.object.type>",
				convert.expandAnyNode("ns:type.object.type"));
		assertEquals("\"Number\"@en",
				convert.expandAnyNode("\"Number\"@en"));
		
	}
	
	@Test
	public void wholeSystemTest() throws Exception {
		ListSink<PrimitiveTriple> innerSink=new ListSink<PrimitiveTriple>();
		ExpandFreebaseRdfToNTriples convert=new ExpandFreebaseRdfToNTriples(innerSink,new NullSink<String>());
		populateFreebasePrefixes(convert);
		convert.accept("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
		List<PrimitiveTriple> output=innerSink.getContent();
		assertEquals(1,output.size());
		assertEquals("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>",output.get(0).subject);
		assertEquals("<http://rdf.freebase.com/ns/type.property.unique>",output.get(0).predicate);
		assertEquals("true",output.get(0).object);
	}
	
	
	
	protected void populateFreebasePrefixes(ExpandFreebaseRdfToNTriples that) throws Exception {
		that.accept("@prefix ns: <http://rdf.freebase.com/ns/>.");
		that.accept("@prefix key: <http://rdf.freebase.com/key/>.");
		that.accept("@prefix owl: <http://www.w3.org/2002/07/owl#>.");
		that.accept("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
		that.accept("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");
		that.accept("@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.");
	};
}
