package com.ontology2.millipede.triples;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.millipede.primitiveTriples.PartitionPrimitiveTripleOnSubject;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;

public class PartitionOnSubjectTTest {

	private PartitionPrimitiveTripleOnSubject reference;
	private PartitionOnSubjectT that;
	
	@Before
	public void setup() {
		reference = new PartitionPrimitiveTripleOnSubject(1024);
		that = new PartitionOnSubjectT(1024);
	}
	
	@Test
	public void testBin() {
		final String nodeValue="http://basekb.com/";
		final String predicate="http://basekb.com/predicate";
		
		final PrimitiveTriple pt=new PrimitiveTriple(
				"<"+nodeValue+">", 
				"<"+predicate+">", 
				"55");
		
		final Triple t=new Triple(
				Node.createURI(nodeValue), 
				Node.createURI(predicate), 
				Node.createLiteral("55",XSDDatatype.XSDinteger));
		
		final int binRef=reference.bin(pt);
		final int bin=that.bin(t);
		assertEquals(binRef,bin);
		
	}

}
