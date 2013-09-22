package com.ontology2.bakemono.primitiveTriples;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;

public class TestPrimitiveTripleCodec {

    private PrimitiveTripleCodec codec;

    @Before 
    public void setup() {
        codec=new PrimitiveTripleCodec();
    }
    
    
    final static String X="<http://example.com/X>";
    final static String Y="<http://example.com/Y>";
    final static String Z="<http://example.com/Z>";
    final static String L1="88712";
    final static String L2="\"all is well that ends well\"";
    @Test
    public void parsesALinkTriple() {
        String in=X+"       "+Y+"\t"+Z+".";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(Z,out.getObject());
    }
    
    @Test
    public void parsesASingleSpaceTriple() {
        String in=X+" "+Y+" "+Z+" .";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(Z,out.getObject());
    }
    
    @Test
    public void parsesATabbyTriple() {
        String in=X+"\t"+Y+"\t "+Z+"\t.";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(Z,out.getObject());
    }

    @Test
    public void parsesALiteralTriple() {
        String in=X+"\t"+Y+"\t "+L1+"\t.";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(L1,out.getObject());
    }
    
    @Test
    public void parsesALiteralTripleWithNoSpaceBeforeTerminalPeriod() {
        String in=X+"\t"+Y+"\t "+L1+".";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(L1,out.getObject());
    }
    
    @Test
    public void parsesALiteralValueWithSpaces() {
        String in=X+"\t"+Y+"\t "+L2+"\t.";
        PrimitiveTriple out=codec.decode(in);
        assertEquals(X,out.getSubject());
        assertEquals(Y,out.getPredicate());
        assertEquals(L2,out.getObject());
    }
}
