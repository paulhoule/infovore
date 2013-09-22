package com.ontology2.bakemono.sieve3;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Test;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.base.Predicate;
import com.ontology2.bakemono.mapred.RealMultipleOutputs;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;

public class Sieve3MapperTest {

    @Test
    public void itClosesMultipleOutputs() throws IOException, InterruptedException {
        Sieve3Mapper s3m=new Sieve3Mapper();
        s3m.mos=mock(RealMultipleOutputs.class);
        s3m.cleanup(mock(Context.class));
        verify(s3m.mos).close();
    }
    
    @Test
    public void linkTriplesAreAcceptedAsSuch() {
        PrimitiveTriple pt=new PrimitiveTriple(
                "<http://example.com/A>",
                "<http://example.com/B>",
                "<http://example.com/C>"
                );
        
        assertTrue(Sieve3Tool.isLinkRelationship().apply(pt));
    }
    
    @Test
    public void nonLinkTriplesTriplesAreNotAcceptedAsSuch() {
        PrimitiveTriple pt=new PrimitiveTriple(
                "<http://example.com/A>",
                "<http://example.com/B>",
                "55"
                );
        
        assertFalse(Sieve3Tool.isLinkRelationship().apply(pt));
    }

}
