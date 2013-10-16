package com.ontology2.rdf;
import static org.junit.Assert.*;

import org.junit.Test;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.XSD;
import com.ontology2.rdf.Economizer;
import com.ontology2.rdf.CacheEconomizer;
import com.ontology2.rdf.TripleEconomizer;


public class UnderstandNodeMemoryBehavior {

    @Test
    public void identicallyNamedNodesAreNotSameObject() {
        Node uriA=Node.createURI("http://slashdot.org/");
        Node uriB=Node.createURI("http://slashdot.org/");
        assertTrue(uriA.equals(uriB));
        assertTrue(uriA!=uriB);
    }

    @Test
    public void theEconomizerChangesThat() {
        Economizer<Node> e=new CacheEconomizer<Node>();
        Node uriA=e.economize(Node.createURI("http://slashdot.org/"));
        Node uriB=e.economize(Node.createURI("http://slashdot.org/"));
        assertTrue(uriA==uriB);
    }

    @Test
    public void theEconomizerDoesntSquashOtherNodes() {
        Economizer<Node> e=new CacheEconomizer<Node>();
        Node uriA=e.economize(Node.createURI("http://slashdot.org/1"));
        Node uriB=e.economize(Node.createURI("http://slashdot.org/2"));
        assertTrue(uriA!=uriB);
    }

    @Test
    public void economizingTriplesEconomizesTheNodes() {
        Node s1a=Node.createURI("http://example.com/s1");
        Node s1b=Node.createURI("http://example.com/s1");
        Node p1a=Node.createURI("http://example.com/p1");
        Node p1b=Node.createURI("http://example.com/p1");
        Node o1a=Node.createLiteral("55",XSDDatatype.XSDint);
        Node o1b=Node.createLiteral("55",XSDDatatype.XSDint);

        assertTrue(o1a!=o1b);

        Triple t1=new Triple(s1a,p1a,o1a);
        Triple t2=new Triple(s1b,p1b,o1b);
        assertTrue(t1!=t2);
        assertEquals(t1,t2);

        Economizer<Triple> e=new TripleEconomizer();

        Triple t3=e.economize(t1);
        Triple t4=e.economize(t2);

        assertTrue(t3.getSubject()==t4.getSubject());
        assertTrue(t3.getObject()==t4.getObject());
        assertTrue(t3.getPredicate()==t4.getPredicate());

        assertTrue(s1a.equals(t3.getSubject()));
        assertTrue(p1a.equals(t3.getPredicate()));
        assertTrue(o1a.equals(t3.getObject()));

    }
}
