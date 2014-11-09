package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import org.junit.Test;

import java.io.IOException;

import static com.ontology2.bakemono.jena.TestWritableNode.roundtrip;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestWritableTriple {
    @Test
    public void canRoundtripALinkTriple() throws IOException {
        Node s=Node.createURI("http://example.com/A1");
        Node p=Node.createURI("http://example.com/B2");
        Node o=Node.createURI("http://example.com/C3");
        
        Triple source=new Triple(s,p,o);
        WritableTriple wt1=new WritableTriple(source);
        WritableTriple wt2=new WritableTriple(null);
        
        roundtrip(wt1,wt2);
        Triple destination=wt2.getTriple();
        assertEquals(source,destination);
    }
    
    @Test
    public void twoTriplesWithTheSamePOAreNotEqual() {
        WritableTriple croatianKey=new WritableTriple(new Triple(
                Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                ,Node_URI.createURI("http://rdf.basekb.com/ns/type.object.key")
                ,Node.createLiteral("/wikipedia/hr_title/Arnold_Schwarzenegger")
        ));
        
        WritableTriple polishKey=new WritableTriple(new Triple(
                Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                ,Node_URI.createURI("http://rdf.basekb.com/ns/type.object.key")
                ,Node.createLiteral("/wikipedia/pl/Arnold_Schwarzenegger")
        ));
        
        assertFalse(0==croatianKey.compareTo(polishKey));
    };
}
