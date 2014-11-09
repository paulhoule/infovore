package com.ontology2.bakemono.jena;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;

public class TestPartitionerOnSubject {
    public final Node A= Node_URI.createURI("http://example.com/A");
    public final Node B= Node_URI.createURI("http://example.com/B");
    public final Node C= Node_URI.createURI("http://example.com/C");

    PartitionOnSubject that;
    int NPARTITIONS=111;

    @Before
    public void setup() {
        that=new PartitionOnSubject();
    }

    @Test
    public void triplesWithSameSPartitionTheSame() {
        assertEquals(partition(A, B, C), partition(A, C, B));
        assertEquals(partition(B, B, C), partition(B, B, B));
        assertEquals(partition(C, B, C), partition(C, A, A));
    }

    @Test
    public void twoTriplesWithDifferentSPartitionDifferently() {
        assertFalse(partition(A, B, C) == partition(C, C, B));
    }

    int partition(Node x,Node y,Node z) {
        WritableTriple wt=new WritableTriple(new Triple(x,y,z));
        return that.getPartition(wt,wt,NPARTITIONS);
    }
}
