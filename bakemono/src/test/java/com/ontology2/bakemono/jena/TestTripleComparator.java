package com.ontology2.bakemono.jena;

import static org.junit.Assert.*;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;

public class TestTripleComparator {
    public final Node A=Node_URI.createURI("http://example.com/A");
    public final Node B=Node_URI.createURI("http://example.com/B");
    public final Node C=Node_URI.createURI("http://example.com/C");
    
    public final Triple base=new Triple(A,B,C);
    public final TripleComparator c=new TripleComparator();
    
    @Test
    public void testABC() {
        assertEquals(0,c.compare(base, new Triple(A,B,C)));
    }
    
    @Test
    public void testABA() {
        assertTrue(c.compare(base, new Triple(A,B,A))>0);
    }
    
    @Test
    public void testABB() {
        assertTrue(c.compare(base, new Triple(A,B,B))>0);
    }
    
    @Test
    public void testABBreversed() {
        assertTrue(c.compare(new Triple(A,B,B),base)<0);
    }
    
    @Test
    public void testCBC() {
        assertTrue(c.compare(base,new Triple(C,B,C))<0);
    }
    
    @Test
    public void testCCB() {
        assertTrue(c.compare(base,new Triple(C,C,B))<0);
    }
    
    @Test
    public void testAAC() {
        assertTrue(c.compare(base,new Triple(A,A,C))>0);
    }
    
    @Test
    public void testACC() {
        assertTrue(c.compare(base,new Triple(A,C,C))<0);
    }
    
}
