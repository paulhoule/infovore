package com.ontology2.bakemono.configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;

public class TestBeans {
    @Test
    public void linkTriplesAreAcceptedAsSuchByFunction() {
        PrimitiveTriple pt=new PrimitiveTriple(
                "<http://example.com/A>",
                "<http://example.com/B>",
                "<http://example.com/C>"
                );
        
        assertTrue(Beans.isLinkRelationship().apply(pt));
    }
    
    @Test
    public void nonLinkTriplesTriplesAreNotAcceptedAsSuchByFunction() {
        PrimitiveTriple pt=new PrimitiveTriple(
                "<http://example.com/A>",
                "<http://example.com/B>",
                "55"
                );
        
        assertFalse(Beans.isLinkRelationship().apply(pt));
    }
}
