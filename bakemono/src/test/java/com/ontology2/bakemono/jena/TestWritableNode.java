package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.*;

import static com.google.common.base.Strings.repeat;
import static org.junit.Assert.assertEquals;

public class TestWritableNode {
    @Test
    public void serializeAndDeserializeURI() throws IOException {
        Node n1=Node.createURI("http://example.com/ZZZ");
        assertEquals("http://example.com/ZZZ",n1.getURI());
        
        WritableNode wn1=new WritableNode(n1);
        WritableNode wn2=new WritableNode(null);

        roundtrip(wn1, wn2);

        assertEquals("http://example.com/ZZZ",wn2.getNode().getURI());
        
    }
    
    @Test
    public void serializeAndDeserializeInteger() throws IOException {
        Node n1= NodeFactory.createLiteral("77641",XSDDatatype.XSDinteger);
        assertEquals(XSDDatatype.XSDinteger,n1.getLiteralDatatype());
        WritableNode wn1=new WritableNode(n1);
        WritableNode wn2=new WritableNode(null);

        roundtrip(wn1, wn2);
        assertEquals(XSDDatatype.XSDinteger,wn2.getNode().getLiteralDatatype());
        assertEquals(77641,wn2.getNode().getLiteralValue());
    }
    
    @Test
    public void serializeAndDeserializeStringWithLanguage() throws IOException {
        Node n1=Node.createLiteral("kore wa okane desu", "jp", false);

        WritableNode wn1=new WritableNode(n1);
        WritableNode wn2=new WritableNode(null);

        roundtrip(wn1, wn2);
        assertEquals("kore wa okane desu",wn2.getNode().getLiteralValue());
        assertEquals("jp", wn2.getNode().getLiteralLanguage());
    }
    
    @Test
    public void serializeAndDeserializeStringWithoutLanguage() throws IOException {
        Node n1=Node.createLiteral("jjshsbn7");
        WritableNode wn1=new WritableNode(n1);
        WritableNode wn2=new WritableNode(null);

        roundtrip(wn1, wn2);
        assertEquals("jjshsbn7",wn2.getNode().getLiteralValue());
        assertEquals("", wn2.getNode().getLiteralLanguage());
    }

    @Test
    public void serializeAndDeserialize100_000CharString() throws IOException {
        String bigString= repeat("TEN DIGITS", 10000);
        assertEquals((int) Math.pow(10,5),bigString.length());
        Node n1=Node.createLiteral(bigString);
        WritableNode wn1=new WritableNode(n1);
        WritableNode wn2=new WritableNode(null);

        roundtrip(wn1, wn2);
        assertEquals(bigString,wn2.getNode().getLiteralValue());
        assertEquals("", wn2.getNode().getLiteralLanguage());
    }

    public static void roundtrip(Writable wn1, Writable wn2)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(bos);
        wn1.write(out);
        byte[] result=bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(result);
        DataInput in=new DataInputStream(bis);
        wn2.readFields(in);
    }
}
