package com.ontology2.bakemono.primitiveTriples;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleTypeRewriter;

public class PrimitiveTripleTypeRewriterTest {

    private Function<PrimitiveTriple,PrimitiveTriple> rewriter;

    @Before
    public void setup() {
        rewriter = new PrimitiveTripleTypeRewriter(
                "xsd:datetime",
                "<http://rdf.ontology2.com/freebaseDate>"
        );
    }
    
    @Test
    public void ordinaryTriplesPassThrough() {
       PrimitiveTriple p1=new PrimitiveTriple("<http://example.com/ats>","<http://example.com/unlocksWithNumber>","\"true\"^^xsd:boolean");
       PrimitiveTriple p2=rewriter.apply(p1);
       assertEquals(p1,p2);
    }
    
    @Test
    public void datetimeIsRewritten() {
       PrimitiveTriple p1=new PrimitiveTriple("<http://example.com/ats>","<http://example.com/oxygenSensorBurnedOutOn>","\"true\"^^xsd:datetime");
       PrimitiveTriple p2=rewriter.apply(p1);
       assertNotEquals(p1,p2);
    }

}
